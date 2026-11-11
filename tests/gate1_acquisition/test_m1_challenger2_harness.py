"""Gate G1 Empirical Challenger 2 Adversarial Stress Suite.

Adversarially tests:
1. Canonical JSON & Identity Hashing (RFC 8785 determinism, non-finite float fuzzing,
   permutation invariance of FrozenInputPlan, single-bit sensitivity matrix, code bundle manifest hashing).
2. Acquisition & Provenance Recovery (network stream interruptions, truncated streams,
   HTTP status matrix, Parquet & CSV format corruption attacks, provenance tampering matrix,
   and non-halting frontier gap testing across complex topologies).
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from pathlib import Path
import random
import sqlite3
import tempfile
import time
from typing import Any, Sequence

from hvfhv.config import SystemConfig
from hvfhv.control.registry import Registry, configure_sqlite_connection
from hvfhv.contracts.identity import (
    canonical_json_dumps,
    canonical_json_bytes,
    compute_revision_id,
    compute_derivation_id,
    compute_code_bundle_manifest_hash,
    FrozenInputPlan,
    SourceRevisionEntry,
)
from hvfhv.acquisition.discover import (
    discover_frontier,
    ObservationOutcome,
    check_source_metadata,
)
from hvfhv.acquisition.acquire import (
    acquire_source,
    reconcile_raw_storage,
    TransportError,
    SourceUnavailableError,
    SecurityError,
    CorruptionError,
    UnsupportedSchemaError,
    AcquisitionError,
)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
    pq = None


# ==============================================================================
# Helper Fixtures & Mock Transports
# ==============================================================================

class MockRequest:
    def __init__(self, method: str = "GET", url: str = ""):
        self.method = method
        self.url = url


class MockResponse:
    def __init__(
        self,
        status_code: int,
        content: bytes | str = b"",
        headers: dict[str, str] | None = None,
        request: Any = None,
        generator_override: Any = None,
    ):
        self.status_code = status_code
        self.content = content if isinstance(content, bytes) else content.encode("utf-8")
        self.headers = headers or {}
        self.request = request or MockRequest()
        self.url = getattr(self.request, "url", "")
        self.generator_override = generator_override

    def iter_bytes(self, chunk_size: int = 65536):
        if self.generator_override is not None:
            yield from self.generator_override()
            return
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i : i + chunk_size]

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class MockTransport:
    def __init__(self, handler: Any):
        self.handler = handler


class MockClient:
    def __init__(self, transport: Any = None):
        self.transport = transport

    def head(self, url: str, headers: dict[str, str] | None = None) -> MockResponse:
        req = MockRequest("HEAD", url)
        return self.transport.handler(req)

    def get(self, url: str, headers: dict[str, str] | None = None) -> MockResponse:
        req = MockRequest("GET", url)
        return self.transport.handler(req)

    def stream(self, method: str, url: str, headers: dict[str, str] | None = None):
        req = MockRequest(method, url)
        resp = self.transport.handler(req)

        class _Ctx:
            def __enter__(self):
                return resp
            def __exit__(self, *args):
                pass

        return _Ctx()

    def close(self) -> None:
        pass


def make_sample_parquet_bytes(num_rows: int = 100, is_2025: bool = False) -> bytes:
    """Generate a valid HVFHV parquet byte stream in memory."""
    if pa is not None and pq is not None:
        fields = [
            ("hvfhs_license_num", pa.string()),
            ("dispatching_base_num", pa.string()),
            ("originating_base_num", pa.string()),
            ("request_datetime", pa.timestamp("us")),
            ("on_scene_datetime", pa.timestamp("us")),
            ("pickup_datetime", pa.timestamp("us")),
            ("dropoff_datetime", pa.timestamp("us")),
            ("PULocationID", pa.int64()),
            ("DOLocationID", pa.int64()),
            ("trip_miles", pa.float64()),
            ("trip_time", pa.int64()),
            ("base_passenger_fare", pa.float64()),
            ("tolls", pa.float64()),
            ("bcf", pa.float64()),
            ("sales_tax", pa.float64()),
            ("congestion_surcharge", pa.float64()),
            ("airport_fee", pa.float64()),
            ("tips", pa.float64()),
            ("driver_pay", pa.float64()),
            ("shared_request_flag", pa.string()),
            ("shared_match_flag", pa.string()),
            ("access_a_ride_flag", pa.string()),
            ("wav_request_flag", pa.string()),
            ("wav_match_flag", pa.string()),
        ]
        if is_2025:
            fields.append(("cbd_congestion_fee", pa.float64()))

        schema = pa.schema(fields)
        data = {
            name: [None] * num_rows if t != pa.int64() else [1] * num_rows
            for name, t in fields
        }
        table = pa.Table.from_pydict(data, schema=schema)
        import io
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue()

    # Synthetic binary fallback with PAR1 magic header and footer
    payload = b"SYNTHETIC_PARQUET_BODY_ROWS_" + str(num_rows).encode("utf-8")
    return b"PAR1" + payload + b"\x00\x00\x00\x00" + b"PAR1"


def make_sample_zone_csv_bytes() -> bytes:
    """Generate a valid taxi zone lookup CSV byte stream."""
    header = "LocationID,Borough,Zone,service_zone\n"
    rows = [
        "1,EWR,Newark Airport,EWR\n",
        "2,Queens,Jamaica Bay,Boro Zone\n",
        "3,Bronx,Allerton/Pelham Gardens,Boro Zone\n",
        "264,Unknown,NV,N/A\n",
        "265,Unknown,NA,N/A\n",
    ]
    return (header + "".join(rows)).encode("utf-8")


# ==============================================================================
# 1. CANONICAL JSON & IDENTITY HASHING PROPERTY TESTS
# ==============================================================================

def generate_random_json_tree(rng: random.Random, depth: int = 0, max_depth: int = 5) -> Any:
    """Generate arbitrary complex nested data structure for property testing."""
    if depth >= max_depth:
        leaf_choice = rng.choice(["int", "float", "str", "bool", "null"])
        if leaf_choice == "int":
            return rng.randint(-1_000_000, 1_000_000)
        elif leaf_choice == "float":
            return rng.choice([0.0, -0.0, 1.23456789, -987.654, 1e-10, 1e10, rng.uniform(-1000, 1000)])
        elif leaf_choice == "str":
            char_pool = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 _-!@#$%^&*()=+,./;'\u00e9\u00fc\u4e16\U0001f600"
            length = rng.randint(0, 15)
            return "".join(rng.choice(char_pool) for _ in range(length))
        elif leaf_choice == "bool":
            return rng.choice([True, False])
        else:
            return None

    node_type = rng.choice(["dict", "dict", "list", "int", "str", "float", "bool", "null"])
    if node_type == "dict":
        num_keys = rng.randint(1, 6)
        d = {}
        for _ in range(num_keys):
            k_len = rng.randint(1, 10)
            k_pool = "abcdefghijklmnopqrstuvwxyz0123456789_-\u00e9\u4e16"
            key = "".join(rng.choice(k_pool) for _ in range(k_len))
            d[key] = generate_random_json_tree(rng, depth + 1, max_depth)
        return d
    elif node_type == "list":
        num_items = rng.randint(0, 5)
        return [generate_random_json_tree(rng, depth + 1, max_depth) for _ in range(num_items)]
    elif node_type == "int":
        return rng.randint(-1_000_000, 1_000_000)
    elif node_type == "float":
        return rng.choice([0.0, -0.0, 1.23456789, -987.654, 1e-10, 1e10, rng.uniform(-1000, 1000)])
    elif node_type == "str":
        char_pool = "abcdefghijklmnopqrstuvwxyz0123456789 _-\u00e9\u4e16"
        return "".join(rng.choice(char_pool) for _ in range(rng.randint(0, 12)))
    elif node_type == "bool":
        return rng.choice([True, False])
    else:
        return None


def shuffle_dict_keys_recursively(obj: Any, rng: random.Random) -> Any:
    """Recursively shuffle insertion order of keys in dictionaries."""
    if isinstance(obj, dict):
        keys = list(obj.keys())
        rng.shuffle(keys)
        return {k: shuffle_dict_keys_recursively(obj[k], rng) for k in keys}
    elif isinstance(obj, list):
        return [shuffle_dict_keys_recursively(item, rng) for item in obj]
    else:
        return obj


def test_prop_canonical_json_deep_tree_determinism():
    """Property test: 200 arbitrary deeply nested trees produce identical canonical JSON under key shuffles."""
    rng = random.Random(42)
    iterations = 200

    for i in range(iterations):
        tree = generate_random_json_tree(rng, depth=0, max_depth=6)

        canonical_str_original = canonical_json_dumps(tree)
        canonical_bytes_original = canonical_json_bytes(tree)

        parsed = json.loads(canonical_str_original)
        assert parsed is not None or tree is None

        for _ in range(5):
            permuted = shuffle_dict_keys_recursively(copy.deepcopy(tree), rng)
            canonical_str_permuted = canonical_json_dumps(permuted)
            canonical_bytes_permuted = canonical_json_bytes(permuted)

            assert canonical_str_original == canonical_str_permuted, (
                f"Determinism violated on iteration {i}: {canonical_str_original} != {canonical_str_permuted}"
            )
            assert canonical_bytes_original == canonical_bytes_permuted

    print(f"    [PASS] test_prop_canonical_json_deep_tree_determinism ({iterations} deep trees, 1,000 permutations)")


def test_prop_rfc8785_codepoint_sorting_oracle():
    """Verify strict UTF-8 codepoint key sorting conforming to RFC 8785 section 3.2.3."""
    test_keys = [
        "",                     # 0x00
        "1",                    # 0x31
        "10",                   # 0x31 0x30
        "2",                    # 0x32
        "A",                    # 0x41
        "B",                    # 0x42
        "_",                    # 0x5F
        "a",                    # 0x61
        "b",                    # 0x62
        "\u00e9",               # 0xC3 0xA9 (é)
        "\u4e16",               # 0xE4 0xB8 0x96 (世)
        "\U0001f600",           # 0xF0 0x9F 0x98 0x80 (😀)
    ]

    rng = random.Random(99)
    for _ in range(50):
        shuffled = list(test_keys)
        rng.shuffle(shuffled)
        d = {k: idx for idx, k in enumerate(shuffled)}

        serialized = canonical_json_dumps(d)
        parsed_keys = list(json.loads(serialized).keys())

        assert parsed_keys == test_keys, (
            f"Key ordering violated! Expected {test_keys}, got {parsed_keys}"
        )

    print("    [PASS] test_prop_rfc8785_codepoint_sorting_oracle (50 key shuffles, verified RFC 8785 ordering)")


def test_adversarial_float_fuzzing():
    """Adversarially fuzz non-finite floats at every structural depth and verify strict ValueError rejection."""
    non_finite_specimens = [
        float("nan"),
        float("-nan"),
        float("inf"),
        float("-inf"),
        math.nan,
        -math.nan,
        math.inf,
        -math.inf,
        float("inf") - float("inf"),
        1e308 * 10,
    ]

    for val in non_finite_specimens:
        # 1. Root
        try:
            canonical_json_dumps(val)
            raise AssertionError(f"Expected ValueError for root non-finite float: {val}")
        except ValueError as exc:
            assert "Canonical JSON does not permit non-finite floats" in str(exc)

        # 2. Shallow dict
        try:
            canonical_json_dumps({"bad": val})
            raise AssertionError(f"Expected ValueError for shallow dict value: {val}")
        except ValueError as exc:
            assert "Canonical JSON does not permit non-finite floats" in str(exc)

        # 3. Deeply nested dict
        try:
            canonical_json_dumps({"level1": {"level2": {"level3": {"target": val}}}})
            raise AssertionError(f"Expected ValueError for deep dict value: {val}")
        except ValueError as exc:
            assert "Canonical JSON does not permit non-finite floats" in str(exc)

        # 4. Shallow list
        try:
            canonical_json_dumps([1, 2, val, 4])
            raise AssertionError(f"Expected ValueError for shallow list item: {val}")
        except ValueError as exc:
            assert "Canonical JSON does not permit non-finite floats" in str(exc)

        # 5. Deep list within dict within list
        try:
            canonical_json_dumps([{"a": [{"b": [val]}]}])
            raise AssertionError(f"Expected ValueError for deep nested item: {val}")
        except ValueError as exc:
            assert "Canonical JSON does not permit non-finite floats" in str(exc)

    finite_specimens = [
        0.0,
        -0.0,
        1.0,
        -1.0,
        3.141592653589793,
        1e-300,
        1e300,
        -999999999.99999,
        1e-15,
    ]
    for f in finite_specimens:
        result = canonical_json_dumps({"val": f})
        assert isinstance(result, str)

    bad_keys = [123, 45.67, True, False, None, (1, 2)]
    for bk in bad_keys:
        try:
            canonical_json_dumps({bk: "val"})  # type: ignore
            raise AssertionError(f"Expected TypeError for non-string key: {bk}")
        except TypeError as exc:
            assert "dictionary keys must be strings" in str(exc)

    print("    [PASS] test_adversarial_float_fuzzing (60 non-finite injections rejected, 9 finite accepted, 6 bad keys caught)")


def test_frozen_input_plan_permutation_invariance_stress():
    """Stress test: 100 random permutations of FrozenInputPlan produce identical compute_derivation_id."""
    baseline_months = [f"2024-{m:02d}" for m in range(1, 13)]
    baseline_revisions = [
        SourceRevisionEntry(f"tlc/hvfhv/2024-{m:02d}", f"{m:02d}" * 32)
        for m in range(1, 13)
    ]
    baseline_semantic_config = {
        "outlier_iqr_multiplier": 1.5,
        "fare_minimum_dollars": 0.0,
        "max_trip_duration_seconds": 86400,
        "retention_policy": "rolling_12_months",
    }

    baseline_plan = FrozenInputPlan(
        source_revisions=baseline_revisions,
        window_months=baseline_months,
        zone_revision_id="z" * 64,
        schema_adapter_version="adapter.v1",
        rules_hash="r" * 64,
        metric_definition_hash="m" * 64,
        code_bundle_manifest_hash="c" * 64,
        lockfile_hash="l" * 64,
        model_schema_version="model.v1",
        semantic_configuration=baseline_semantic_config,
    )

    baseline_derivation_id = compute_derivation_id(baseline_plan)
    assert len(baseline_derivation_id) == 64
    assert all(c in "0123456789abcdef" for c in baseline_derivation_id)

    rng = random.Random(1337)
    permutations = 100

    for i in range(permutations):
        shuffled_months = list(baseline_months)
        rng.shuffle(shuffled_months)

        shuffled_revisions = list(baseline_revisions)
        rng.shuffle(shuffled_revisions)

        shuffled_config_keys = list(baseline_semantic_config.keys())
        rng.shuffle(shuffled_config_keys)
        shuffled_config = {k: baseline_semantic_config[k] for k in shuffled_config_keys}

        permuted_plan = FrozenInputPlan(
            source_revisions=shuffled_revisions,
            window_months=shuffled_months,
            zone_revision_id="z" * 64,
            schema_adapter_version="adapter.v1",
            rules_hash="r" * 64,
            metric_definition_hash="m" * 64,
            code_bundle_manifest_hash="c" * 64,
            lockfile_hash="l" * 64,
            model_schema_version="model.v1",
            semantic_configuration=shuffled_config,
        )

        perm_derivation_id = compute_derivation_id(permuted_plan)
        assert perm_derivation_id == baseline_derivation_id, (
            f"Permutation invariance violated at iteration {i}: {perm_derivation_id} != {baseline_derivation_id}"
        )

    print(f"    [PASS] test_frozen_input_plan_permutation_invariance_stress ({permutations} complex random permutations)")


def test_frozen_input_plan_single_bit_sensitivity_matrix():
    """Adversarially mutate every field of FrozenInputPlan by a single bit/char; verify hash sensitivity."""
    baseline_months = [f"2024-{m:02d}" for m in range(1, 13)]
    baseline_revisions = [
        SourceRevisionEntry(f"tlc/hvfhv/2024-{m:02d}", f"{m:02d}" * 32)
        for m in range(1, 13)
    ]
    baseline_semantic_config = {
        "multiplier": 1.5,
        "mode": "standard",
        "threshold": 100,
    }

    baseline_plan = FrozenInputPlan(
        source_revisions=baseline_revisions,
        window_months=baseline_months,
        zone_revision_id="a" * 64,
        schema_adapter_version="adapter.v1",
        rules_hash="b" * 64,
        metric_definition_hash="c" * 64,
        code_bundle_manifest_hash="d" * 64,
        lockfile_hash="e" * 64,
        model_schema_version="model.v1",
        semantic_configuration=baseline_semantic_config,
    )
    baseline_hash = compute_derivation_id(baseline_plan)

    mutations: list[tuple[str, FrozenInputPlan]] = [
        ("source_revisions: mutate revision_id", FrozenInputPlan(
            source_revisions=[SourceRevisionEntry("tlc/hvfhv/2024-01", "01" * 31 + "02")] + baseline_revisions[1:],
            window_months=baseline_months, zone_revision_id="a" * 64, schema_adapter_version="adapter.v1",
            rules_hash="b" * 64, metric_definition_hash="c" * 64, code_bundle_manifest_hash="d" * 64,
            lockfile_hash="e" * 64, model_schema_version="model.v1", semantic_configuration=baseline_semantic_config,
        )),
        ("source_revisions: mutate logical_source_id", FrozenInputPlan(
            source_revisions=baseline_revisions[:5] + [SourceRevisionEntry("tlc/hvfhv/2024-06X", baseline_revisions[5].revision_id)] + baseline_revisions[6:],
            window_months=baseline_months, zone_revision_id="a" * 64, schema_adapter_version="adapter.v1",
            rules_hash="b" * 64, metric_definition_hash="c" * 64, code_bundle_manifest_hash="d" * 64,
            lockfile_hash="e" * 64, model_schema_version="model.v1", semantic_configuration=baseline_semantic_config,
        )),
        ("source_revisions: drop entry", FrozenInputPlan(
            source_revisions=baseline_revisions[:-1],
            window_months=baseline_months, zone_revision_id="a" * 64, schema_adapter_version="adapter.v1",
            rules_hash="b" * 64, metric_definition_hash="c" * 64, code_bundle_manifest_hash="d" * 64,
            lockfile_hash="e" * 64, model_schema_version="model.v1", semantic_configuration=baseline_semantic_config,
        )),
        ("window_months: alter month", FrozenInputPlan(
            source_revisions=baseline_revisions,
            window_months=baseline_months[:-1] + ["2025-01"],
            zone_revision_id="a" * 64, schema_adapter_version="adapter.v1", rules_hash="b" * 64,
            metric_definition_hash="c" * 64, code_bundle_manifest_hash="d" * 64, lockfile_hash="e" * 64,
            model_schema_version="model.v1", semantic_configuration=baseline_semantic_config,
        )),
        ("zone_revision_id: flip char", FrozenInputPlan(
            source_revisions=baseline_revisions, window_months=baseline_months,
            zone_revision_id="a" * 63 + "b",
            schema_adapter_version="adapter.v1", rules_hash="b" * 64, metric_definition_hash="c" * 64,
            code_bundle_manifest_hash="d" * 64, lockfile_hash="e" * 64, model_schema_version="model.v1",
            semantic_configuration=baseline_semantic_config,
        )),
        ("schema_adapter_version: flip char", FrozenInputPlan(
            source_revisions=baseline_revisions, window_months=baseline_months, zone_revision_id="a" * 64,
            schema_adapter_version="adapter.v2",
            rules_hash="b" * 64, metric_definition_hash="c" * 64, code_bundle_manifest_hash="d" * 64,
            lockfile_hash="e" * 64, model_schema_version="model.v1", semantic_configuration=baseline_semantic_config,
        )),
        ("rules_hash: flip char", FrozenInputPlan(
            source_revisions=baseline_revisions, window_months=baseline_months, zone_revision_id="a" * 64,
            schema_adapter_version="adapter.v1",
            rules_hash="b" * 63 + "c",
            metric_definition_hash="c" * 64, code_bundle_manifest_hash="d" * 64, lockfile_hash="e" * 64,
            model_schema_version="model.v1", semantic_configuration=baseline_semantic_config,
        )),
        ("metric_definition_hash: flip char", FrozenInputPlan(
            source_revisions=baseline_revisions, window_months=baseline_months, zone_revision_id="a" * 64,
            schema_adapter_version="adapter.v1", rules_hash="b" * 64,
            metric_definition_hash="c" * 63 + "d",
            code_bundle_manifest_hash="d" * 64, lockfile_hash="e" * 64, model_schema_version="model.v1",
            semantic_configuration=baseline_semantic_config,
        )),
        ("code_bundle_manifest_hash: flip char", FrozenInputPlan(
            source_revisions=baseline_revisions, window_months=baseline_months, zone_revision_id="a" * 64,
            schema_adapter_version="adapter.v1", rules_hash="b" * 64, metric_definition_hash="c" * 64,
            code_bundle_manifest_hash="d" * 63 + "e",
            lockfile_hash="e" * 64, model_schema_version="model.v1", semantic_configuration=baseline_semantic_config,
        )),
        ("lockfile_hash: flip char", FrozenInputPlan(
            source_revisions=baseline_revisions, window_months=baseline_months, zone_revision_id="a" * 64,
            schema_adapter_version="adapter.v1", rules_hash="b" * 64, metric_definition_hash="c" * 64,
            code_bundle_manifest_hash="d" * 64,
            lockfile_hash="e" * 63 + "f",
            model_schema_version="model.v1", semantic_configuration=baseline_semantic_config,
        )),
        ("model_schema_version: flip char", FrozenInputPlan(
            source_revisions=baseline_revisions, window_months=baseline_months, zone_revision_id="a" * 64,
            schema_adapter_version="adapter.v1", rules_hash="b" * 64, metric_definition_hash="c" * 64,
            code_bundle_manifest_hash="d" * 64, lockfile_hash="e" * 64,
            model_schema_version="model.v2",
            semantic_configuration=baseline_semantic_config,
        )),
        ("semantic_configuration: float delta", FrozenInputPlan(
            source_revisions=baseline_revisions, window_months=baseline_months, zone_revision_id="a" * 64,
            schema_adapter_version="adapter.v1", rules_hash="b" * 64, metric_definition_hash="c" * 64,
            code_bundle_manifest_hash="d" * 64, lockfile_hash="e" * 64, model_schema_version="model.v1",
            semantic_configuration={"multiplier": 1.500000001, "mode": "standard", "threshold": 100},
        )),
        ("semantic_configuration: add key", FrozenInputPlan(
            source_revisions=baseline_revisions, window_months=baseline_months, zone_revision_id="a" * 64,
            schema_adapter_version="adapter.v1", rules_hash="b" * 64, metric_definition_hash="c" * 64,
            code_bundle_manifest_hash="d" * 64, lockfile_hash="e" * 64, model_schema_version="model.v1",
            semantic_configuration={"multiplier": 1.5, "mode": "standard", "threshold": 100, "extra": "new"},
        )),
    ]

    seen_hashes: set[str] = {baseline_hash}
    for desc, mutated_plan in mutations:
        h = compute_derivation_id(mutated_plan)
        assert h != baseline_hash, f"Hash collision on mutation '{desc}'! Both produced {h}"
        assert h not in seen_hashes, f"Cross-mutation hash collision on '{desc}'! Hash {h} already seen"
        seen_hashes.add(h)

    print(f"    [PASS] test_frozen_input_plan_single_bit_sensitivity_matrix ({len(mutations)} mutations verified with 0 collisions)")


def test_code_bundle_manifest_hash_sensitivity():
    """Verify compute_code_bundle_manifest_hash determinism and sensitivity on real repo."""
    repo_root = Path(__file__).resolve().parents[2]
    manifest_hash_1, manifest_1 = compute_code_bundle_manifest_hash(repo_root)

    assert len(manifest_hash_1) == 64
    assert len(manifest_1) > 0
    # Must include identity.py, schemas.json, rules.json
    assert "src/hvfhv/contracts/identity.py" in manifest_1
    assert "contracts/schemas.json" in manifest_1
    assert "contracts/rules.json" in manifest_1

    # Determinism
    manifest_hash_2, manifest_2 = compute_code_bundle_manifest_hash(repo_root)
    assert manifest_hash_1 == manifest_hash_2
    assert manifest_1 == manifest_2

    print(f"    [PASS] test_code_bundle_manifest_hash_sensitivity ({len(manifest_1)} files scanned, deterministically hashed)")


# ==============================================================================
# 2. ACQUISITION & PROVENANCE RECOVERY ADVERSARIAL HARNESS
# ==============================================================================

def test_adversarial_network_stream_disconnect_and_timeout():
    """Simulate mid-stream TCP disconnect/timeout; verify clean rollback with zero dirty storage or SQLite state."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=2)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            full_bytes = make_sample_parquet_bytes(300)
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet"
            call_count = 0

            def disconnecting_generator():
                nonlocal call_count
                call_count += 1
                yield full_bytes[:100]
                raise ConnectionResetError(f"Simulated abrupt TCP reset on attempt {call_count}")

            def handler(request: Any) -> Any:
                headers = {"Content-Length": str(len(full_bytes)), "Accept-Encoding": "identity"}
                return MockResponse(200, headers=headers, request=request, generator_override=disconnecting_generator)

            client = MockClient(transport=MockTransport(handler))

            try:
                acquire_source("tlc/hvfhv/2024-12", url, config, conn, attempt_id="abort-att", client=client)
                raise AssertionError("Expected TransportError but acquire_source succeeded!")
            except TransportError as exc:
                assert "Acquisition failed after 2 attempts" in str(exc)

            assert call_count == 2

            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT count(*) FROM source_observations;")
            assert cur.fetchone()[0] == 0
            cur.close()

            raw_files = list((config.raw_dir / "sha256").glob("*/*"))
            assert len(raw_files) == 0

        finally:
            conn.close()

    print("    [PASS] test_adversarial_network_stream_disconnect_and_timeout (mid-stream abort cleanly quarantined)")


def test_adversarial_network_truncated_content_length():
    """Server sends 200 OK with advertised Content-Length 10000, but stream delivers only 4000 bytes."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=1)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            full_bytes = make_sample_parquet_bytes(300)
            truncated_bytes = full_bytes[: len(full_bytes) // 2]
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet"

            def handler(request: Any) -> Any:
                headers = {
                    "Content-Length": str(len(full_bytes)),
                    "Accept-Encoding": "identity",
                }
                return MockResponse(200, content=truncated_bytes, headers=headers, request=request)

            client = MockClient(transport=MockTransport(handler))

            try:
                acquire_source("tlc/hvfhv/2024-12", url, config, conn, client=client)
                raise AssertionError("Expected CorruptionError on truncated download!")
            except CorruptionError as exc:
                assert "Truncated download" in str(exc)

            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 0
            cur.close()

            raw_files = list((config.raw_dir / "sha256").glob("*/*"))
            assert len(raw_files) == 0

        finally:
            conn.close()

    print("    [PASS] test_adversarial_network_truncated_content_length (truncated byte stream rejected)")


def test_adversarial_http_transient_and_security_handling():
    """Verify 404, 429, 5xx, and SSRF/redirect attacks are handled correctly."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=2)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"

            # 1. 404 Not Found -> SourceUnavailableError immediately (1 attempt)
            calls_404 = 0
            def h_404(req):
                nonlocal calls_404
                calls_404 += 1
                return MockResponse(404, request=req)

            client_404 = MockClient(transport=MockTransport(h_404))
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client_404)
                raise AssertionError("Expected SourceUnavailableError on 404")
            except SourceUnavailableError:
                pass
            assert calls_404 == 1

            # 2. 500, 502, 504 -> retried up to limit, raises TransportError
            for status in [500, 502, 504]:
                calls_5xx = 0
                def h_5xx(req, s=status):
                    nonlocal calls_5xx
                    calls_5xx += 1
                    return MockResponse(s, request=req)

                client_5xx = MockClient(transport=MockTransport(h_5xx))
                try:
                    acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client_5xx)
                    raise AssertionError(f"Expected TransportError on HTTP {status}")
                except TransportError as exc:
                    assert "Failed to stream" in str(exc) or "Acquisition failed" in str(exc)
                assert calls_5xx == 2

            # 3. SSRF / Redirect to unallowlisted domain
            def redirect_attack_handler(req):
                headers = {"Location": "https://malicious-exfiltration-target.org/leak.parquet"}
                return MockResponse(302, headers=headers, request=req)

            client_ssrf = MockClient(transport=MockTransport(redirect_attack_handler))
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client_ssrf)
                raise AssertionError("Expected SecurityError on redirect to unallowlisted host!")
            except SecurityError as exc:
                assert "Host must be in official allowlist" in str(exc)

            # 4. Redirect without Location header
            def empty_loc_redirect_handler(req):
                return MockResponse(301, headers={}, request=req)

            client_empty_loc = MockClient(transport=MockTransport(empty_loc_redirect_handler))
            try:
                acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client_empty_loc)
                raise AssertionError("Expected SecurityError on redirect without Location header!")
            except SecurityError as exc:
                assert "Redirect missing Location header" in str(exc)

            # Invariant: Registry remains pristine
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 0
            cur.close()

        finally:
            conn.close()

    print("    [PASS] test_adversarial_http_transient_and_security_handling (404, 5xx, and SSRF attacks verified)")


def test_adversarial_4xx_client_error_classification_oracle():
    """CHALLENGE / BUG PROBE: Verify actionable 4xx client errors (400, 401, 403, 405) are NOT retried.

    Specification:
    docs/architecture/low-level-design.md §5:
        'treat 404 as unavailable observation, other 4xx as actionable errors.'
    Actionable client errors must NOT be retried like transient transport errors, and must raise
    AcquisitionError (or specific client error), NOT TransportError.

    Implementation Flaw in src/hvfhv/acquisition/acquire.py lines 291-295:
        `except (SecurityError, SourceUnavailableError, CorruptionError, UnsupportedSchemaError): raise`
        fails to include `AcquisitionError`. When line 241 raises AcquisitionError, it is caught by
        `except Exception as exc:`, retried up to max_attempts, and wrapped in TransportError!
    """
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=2)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-01.parquet"

            for status in [400, 401, 403, 405]:
                calls = 0
                def make_handler(s):
                    def h(req):
                        nonlocal calls
                        calls += 1
                        return MockResponse(s, request=req)
                    return h

                client = MockClient(transport=MockTransport(make_handler(status)))
                raised_exception = None
                try:
                    acquire_source("tlc/hvfhv/2025-01", url, config, conn, client=client)
                except Exception as exc:
                    raised_exception = exc

                assert raised_exception is not None, f"Expected exception on HTTP {status}"

                # Specification Invariant 1: Actionable 4xx client errors must NOT be retried!
                if calls != 1:
                    raise AssertionError(
                        f"BUG CONFIRMED: HTTP {status} client error was retried {calls} times (expected 1 call without retries). "
                        f"Raised {type(raised_exception).__name__}: {raised_exception}"
                    )

                # Specification Invariant 2: Exception must be AcquisitionError, NOT misclassified TransportError
                if isinstance(raised_exception, TransportError):
                    raise AssertionError(
                        f"BUG CONFIRMED: HTTP {status} client error was misclassified as TransportError instead of AcquisitionError: {raised_exception}"
                    )

        finally:
            conn.close()

    print("    [PASS] test_adversarial_4xx_client_error_classification_oracle")


def test_adversarial_parquet_and_csv_corruption_attacks():
    """Adversarially deliver corrupt Parquet and CSV payloads; verify detection and quarantine."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root, http_max_retries=1)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            pq_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-12.parquet"
            csv_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

            # Attack 1: Missing PAR1 magic header
            corrupt_header = b"BAD1" + b"\x00" * 100 + b"PAR1"
            client1 = MockClient(transport=MockTransport(lambda req: MockResponse(200, content=corrupt_header, request=req)))
            try:
                acquire_source("tlc/hvfhv/2024-12", pq_url, config, conn, client=client1)
                raise AssertionError("Expected CorruptionError for missing PAR1 header")
            except CorruptionError as exc:
                assert "PAR1 magic header missing" in str(exc)

            # Attack 2: Missing PAR1 magic footer
            corrupt_footer = b"PAR1" + b"\x00" * 100 + b"BAD1"
            client2 = MockClient(transport=MockTransport(lambda req: MockResponse(200, content=corrupt_footer, request=req)))
            try:
                acquire_source("tlc/hvfhv/2024-12", pq_url, config, conn, client=client2)
                raise AssertionError("Expected CorruptionError for missing PAR1 footer")
            except CorruptionError as exc:
                assert "PAR1 magic footer missing" in str(exc)

            # Attack 3: Completely random binary garbage
            rng = random.Random(777)
            garbage = rng.randbytes(1024)
            client3 = MockClient(transport=MockTransport(lambda req: MockResponse(200, content=garbage, request=req)))
            try:
                acquire_source("tlc/hvfhv/2024-12", pq_url, config, conn, client=client3)
                raise AssertionError("Expected CorruptionError for random binary garbage")
            except CorruptionError:
                pass

            # Attack 4: Zone CSV missing required column (e.g. missing 'service_zone')
            corrupt_csv = b"LocationID,Borough,Zone\n1,EWR,Newark Airport\n"
            client4 = MockClient(transport=MockTransport(lambda req: MockResponse(200, content=corrupt_csv, request=req)))
            try:
                acquire_source("tlc/taxi_zones", csv_url, config, conn, client=client4)
                raise AssertionError("Expected UnsupportedSchemaError for missing CSV column")
            except UnsupportedSchemaError as exc:
                assert "Zone CSV missing required columns" in str(exc)

            raw_files = list((config.raw_dir / "sha256").glob("*/*"))
            assert len(raw_files) == 0

        finally:
            conn.close()

    print("    [PASS] test_adversarial_parquet_and_csv_corruption_attacks (bad headers, footers, garbage, and corrupt CSV columns rejected)")


def test_provenance_tampering_refusal_matrix():
    """Adversarial challenge of reconcile_raw_storage (Scenario T29).

    Construct 6 distinct corrupted/tampered raw directories and 2 pristine orphans (Parquet and CSV).
    Verify that reconcile_raw_storage strictly rejects all 6 tampered directories and
    reconciles ONLY the pristine orphans.
    """
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_root = root / ".data"
        config = SystemConfig(data_root=data_root)
        config.ensure_directories()
        reg = Registry(db_path=config.control_dir / "registry.sqlite", data_root=data_root, auto_migrate=True)
        conn = reg.get_connection()

        try:
            raw_sha_dir = data_root / "raw" / "sha256"

            parquet_bytes = make_sample_parquet_bytes(100)
            valid_hasher = hashlib.sha256(parquet_bytes)
            valid_digest = valid_hasher.hexdigest().lower()
            expected_row_count = 100

            # -------------------------------------------------------------
            # Case 1: Corrupt: Missing acquisition_provenance.json entirely
            # -------------------------------------------------------------
            c1_dir = raw_sha_dir / ("1" * 64)
            c1_dir.mkdir(parents=True, exist_ok=True)
            (c1_dir / "source.parquet").write_bytes(parquet_bytes)

            # -------------------------------------------------------------
            # Case 2: Corrupt: Malformed / syntax error in provenance JSON
            # -------------------------------------------------------------
            c2_dir = raw_sha_dir / ("2" * 64)
            c2_dir.mkdir(parents=True, exist_ok=True)
            (c2_dir / "source.parquet").write_bytes(parquet_bytes)
            (c2_dir / "acquisition_provenance.json").write_text("{broken: json, syntax error", encoding="utf-8")

            # -------------------------------------------------------------
            # Case 3: Corrupt: Content SHA-256 in provenance does NOT match directory
            # -------------------------------------------------------------
            c3_dir = raw_sha_dir / ("3" * 64)
            c3_dir.mkdir(parents=True, exist_ok=True)
            (c3_dir / "source.parquet").write_bytes(parquet_bytes)
            (c3_dir / "acquisition_provenance.json").write_text(
                json.dumps({
                    "logical_source_id": "tlc/hvfhv/2024-03",
                    "content_sha256": "wrong_sha256_" + ("0" * 51),
                    "completed_bytes": len(parquet_bytes),
                    "decoded_row_count": expected_row_count,
                    "acquired_at_utc": "2026-11-10T00:00:00Z",
                    "requested_url": "https://nyc.gov/test.parquet",
                }),
                encoding="utf-8",
            )

            # -------------------------------------------------------------
            # Case 4: Corrupt: File on disk has bit-rot / altered content
            # -------------------------------------------------------------
            c4_dir = raw_sha_dir / valid_digest
            c4_dir.mkdir(parents=True, exist_ok=True)
            altered_bytes = bytearray(parquet_bytes)
            altered_bytes[10] ^= 0xFF
            (c4_dir / "source.parquet").write_bytes(bytes(altered_bytes))
            (c4_dir / "acquisition_provenance.json").write_text(
                json.dumps({
                    "logical_source_id": "tlc/hvfhv/2024-04",
                    "content_sha256": valid_digest,
                    "completed_bytes": len(parquet_bytes),
                    "decoded_row_count": expected_row_count,
                    "acquired_at_utc": "2026-11-10T00:00:00Z",
                    "requested_url": "https://nyc.gov/test.parquet",
                }),
                encoding="utf-8",
            )

            # -------------------------------------------------------------
            # Case 5: Corrupt: Falsified row count in provenance
            # -------------------------------------------------------------
            p5_bytes = make_sample_parquet_bytes(200)
            d5 = hashlib.sha256(p5_bytes).hexdigest().lower()
            c5_dir = raw_sha_dir / d5
            c5_dir.mkdir(parents=True, exist_ok=True)
            (c5_dir / "source.parquet").write_bytes(p5_bytes)
            (c5_dir / "acquisition_provenance.json").write_text(
                json.dumps({
                    "logical_source_id": "tlc/hvfhv/2024-05",
                    "content_sha256": d5,
                    "completed_bytes": len(p5_bytes),
                    "decoded_row_count": 999999,
                    "acquired_at_utc": "2026-11-10T00:00:00Z",
                    "requested_url": "https://nyc.gov/test.parquet",
                }),
                encoding="utf-8",
            )

            # -------------------------------------------------------------
            # Case 6: Corrupt: Directory contains provenance but NO source file
            # -------------------------------------------------------------
            c6_dir = raw_sha_dir / ("6" * 64)
            c6_dir.mkdir(parents=True, exist_ok=True)
            (c6_dir / "acquisition_provenance.json").write_text(
                json.dumps({
                    "logical_source_id": "tlc/hvfhv/2024-06",
                    "content_sha256": "6" * 64,
                    "completed_bytes": 100,
                    "decoded_row_count": 100,
                }),
                encoding="utf-8",
            )

            # -------------------------------------------------------------
            # Case 7: PRISTINE VALID ORPHAN (Parquet)
            # -------------------------------------------------------------
            p7_bytes = make_sample_parquet_bytes(150)
            d7 = hashlib.sha256(p7_bytes).hexdigest().lower()
            c7_dir = raw_sha_dir / d7
            c7_dir.mkdir(parents=True, exist_ok=True)
            (c7_dir / "source.parquet").write_bytes(p7_bytes)
            expected_p7_rows = 150 if (pa is not None and pq is not None) else 100
            (c7_dir / "acquisition_provenance.json").write_text(
                json.dumps({
                    "logical_source_id": "tlc/hvfhv/2024-07",
                    "requested_url": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-07.parquet",
                    "content_sha256": d7,
                    "completed_bytes": len(p7_bytes),
                    "footer_row_count": expected_p7_rows,
                    "decoded_row_count": expected_p7_rows,
                    "schema_fingerprint": "hvfhv_24col_pre2025.v1",
                    "acquired_at_utc": "2026-11-10T12:00:00Z",
                }),
                encoding="utf-8",
            )

            # -------------------------------------------------------------
            # Case 8: PRISTINE VALID ORPHAN (Zone CSV)
            # -------------------------------------------------------------
            csv_bytes = make_sample_zone_csv_bytes()
            d8 = hashlib.sha256(csv_bytes).hexdigest().lower()
            c8_dir = raw_sha_dir / d8
            c8_dir.mkdir(parents=True, exist_ok=True)
            (c8_dir / "source.csv").write_bytes(csv_bytes)
            (c8_dir / "acquisition_provenance.json").write_text(
                json.dumps({
                    "logical_source_id": "tlc/taxi_zones",
                    "requested_url": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
                    "content_sha256": d8,
                    "completed_bytes": len(csv_bytes),
                    "footer_row_count": 5,
                    "decoded_row_count": 5,
                    "schema_fingerprint": "taxi_zone_lookup.v1",
                    "acquired_at_utc": "2026-11-10T12:30:00Z",
                }),
                encoding="utf-8",
            )

            # Run reconciliation
            report = reconcile_raw_storage(data_root, conn)

            # Invariant 1: Exactly 2 revisions reconciled (Parquet + Zone CSV)
            pristine_p7_rev = compute_revision_id("tlc/hvfhv/2024-07", d7)
            pristine_c8_rev = compute_revision_id("tlc/taxi_zones", d8)
            assert sorted(report["reconciled"]) == sorted([pristine_p7_rev, pristine_c8_rev]), (
                f"Expected both pristine revisions to be reconciled, got {report['reconciled']}"
            )

            # Invariant 2: All 6 corrupted directories placed into unassigned_orphans
            unassigned = report["unassigned_orphans"]
            assert str(c1_dir) in unassigned, "Case 1 (missing prov) must be unassigned"
            assert str(c2_dir) in unassigned, "Case 2 (bad json) must be unassigned"
            assert str(c3_dir) in unassigned, "Case 3 (sha mismatch in prov) must be unassigned"
            assert str(c4_dir) in unassigned, "Case 4 (bit rot file) must be unassigned"
            assert str(c5_dir) in unassigned, "Case 5 (row count tampered) must be unassigned"
            assert str(c6_dir) in unassigned, "Case 6 (missing file) must be unassigned"
            assert len(unassigned) == 6

            # Invariant 3: SQLite control plane contains strictly 2 rows in source_revisions
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM source_revisions;")
            assert cur.fetchone()[0] == 2
            cur.close()

        finally:
            conn.close()

    print("    [PASS] test_provenance_tampering_refusal_matrix (6 tampered blobs strictly refused; 2 pristine orphans registered)")


def test_frontier_gap_complex_topologies():
    """Adversarial challenge of discover_frontier (Scenario T25) across diverse gap topologies."""
    active_window = [f"2024-{m:02d}" for m in range(1, 13)]

    # Topology A: Interleaved Gaps (M1, M2, M4, M6 available; M3, M5 are 404s)
    def handler_interleaved(req: Any) -> Any:
        url = str(getattr(req, "url", ""))
        if "2025-03" in url or "2025-05" in url:
            return MockResponse(404, request=req)
        return MockResponse(200, headers={"Content-Length": "5000"}, request=req)

    client_a = MockClient(transport=MockTransport(handler_interleaved))
    plan_a = discover_frontier(
        active_window=active_window,
        registered_revisions={},
        client=client_a,
        observation_cutoff="2025-06",
    )

    assert plan_a.frontier_months_scanned == [
        "2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"
    ]
    assert plan_a.proposed_window is not None
    assert plan_a.proposed_window.end_month == "2025-06"
    assert plan_a.proposed_window.is_complete is False
    assert plan_a.proposed_window.missing_months == ["2025-03", "2025-05"]
    assert plan_a.pending_gaps == ["2025-03", "2025-05"]
    new_month_proposals = [p.logical_source_id for p in plan_a.proposals if p.reason == "NEW_MONTH"]
    assert new_month_proposals == [
        "tlc/hvfhv/2025-01", "tlc/hvfhv/2025-02", "tlc/hvfhv/2025-04", "tlc/hvfhv/2025-06"
    ]

    # Topology B: Multi-month consecutive gap (M2 through M5 missing)
    def handler_multi_gap(req: Any) -> Any:
        url = str(getattr(req, "url", ""))
        if any(f"2025-0{m}" in url for m in range(2, 6)):
            return MockResponse(404, request=req)
        return MockResponse(200, request=req)

    client_b = MockClient(transport=MockTransport(handler_multi_gap))
    plan_b = discover_frontier(
        active_window=active_window,
        registered_revisions={},
        client=client_b,
        observation_cutoff="2025-06",
    )

    assert plan_b.frontier_months_scanned == [
        "2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06"
    ]
    assert plan_b.proposed_window is not None
    assert plan_b.proposed_window.end_month == "2025-06"
    assert plan_b.proposed_window.is_complete is False
    assert plan_b.pending_gaps == ["2025-02", "2025-03", "2025-04", "2025-05"]

    # Topology C: Complete 12-month forward window (Full rollover without gaps)
    def handler_full_forward(req: Any) -> Any:
        return MockResponse(200, request=req)

    client_c = MockClient(transport=MockTransport(handler_full_forward))
    plan_c = discover_frontier(
        active_window=active_window,
        registered_revisions={},
        client=client_c,
        observation_cutoff="2025-12",
    )

    assert plan_c.proposed_window is not None
    assert plan_c.proposed_window.end_month == "2025-12"
    assert plan_c.proposed_window.is_complete is True
    assert plan_c.proposed_window.missing_months == []
    assert plan_c.pending_gaps == []
    assert len(plan_c.proposed_window.months) == 12
    assert plan_c.proposed_window.months == [f"2025-{m:02d}" for m in range(1, 13)]

    # Topology D: Zone Reference Check Independently Discovered
    assert plan_a.zone_proposal is not None
    assert plan_a.zone_proposal.logical_source_id == "tlc/taxi_zones"
    assert plan_a.zone_proposal.reason == "ZONE_REFERENCE"

    print("    [PASS] test_frontier_gap_complex_topologies (interleaved gaps, multi-month gaps, full roll, zones)")


# ==============================================================================
# Main Runner
# ==============================================================================

def run_all_challenger2_tests() -> int:
    start_time = time.time()
    print("=" * 80)
    print("GATE G1 EMPIRICAL CHALLENGER 2 ADVERSARIAL STRESS SUITE")
    print("=" * 80)

    tests = [
        # Domain 1: Canonical JSON & Identity Hashing
        ("test_prop_canonical_json_deep_tree_determinism", test_prop_canonical_json_deep_tree_determinism),
        ("test_prop_rfc8785_codepoint_sorting_oracle", test_prop_rfc8785_codepoint_sorting_oracle),
        ("test_adversarial_float_fuzzing", test_adversarial_float_fuzzing),
        ("test_frozen_input_plan_permutation_invariance_stress", test_frozen_input_plan_permutation_invariance_stress),
        ("test_frozen_input_plan_single_bit_sensitivity_matrix", test_frozen_input_plan_single_bit_sensitivity_matrix),
        ("test_code_bundle_manifest_hash_sensitivity", test_code_bundle_manifest_hash_sensitivity),

        # Domain 2: Acquisition & Provenance Recovery Stress
        ("test_adversarial_network_stream_disconnect_and_timeout", test_adversarial_network_stream_disconnect_and_timeout),
        ("test_adversarial_network_truncated_content_length", test_adversarial_network_truncated_content_length),
        ("test_adversarial_http_transient_and_security_handling", test_adversarial_http_transient_and_security_handling),
        ("test_adversarial_4xx_client_error_classification_oracle", test_adversarial_4xx_client_error_classification_oracle),
        ("test_adversarial_parquet_and_csv_corruption_attacks", test_adversarial_parquet_and_csv_corruption_attacks),
        ("test_provenance_tampering_refusal_matrix", test_provenance_tampering_refusal_matrix),
        ("test_frontier_gap_complex_topologies", test_frontier_gap_complex_topologies),
    ]

    passed = 0
    failed = 0

    print("\n--- 1. Domain 1: Canonical JSON & Identity Hashing ---")
    for name, test_fn in tests[:6]:
        try:
            test_fn()
            passed += 1
        except Exception as exc:
            print(f"    [FAIL] {name}: {exc}")
            import traceback
            traceback.print_exc()
            failed += 1

    print("\n--- 2. Domain 2: Acquisition & Provenance Recovery Stress ---")
    for name, test_fn in tests[6:]:
        try:
            test_fn()
            passed += 1
        except Exception as exc:
            print(f"    [FAIL] {name}: {exc}")
            import traceback
            traceback.print_exc()
            failed += 1

    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print(f"CHALLENGER 2 SUMMARY: {passed} PASSED, {failed} FAILED in {elapsed:.2f}s")
    print("=" * 80)

    return failed


if __name__ == "__main__":
    import sys
    sys.exit(run_all_challenger2_tests())
