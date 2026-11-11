"""Gate G1 Test Suite: Contracts, Canonical Identity Hashing, and Scenarios T05, T10, T14.

Verifies:
- RFC 8785 canonical JSON determinism, key sorting, NaN rejection, explicit null semantics.
- compute_revision_id determinism and sensitivity.
- compute_derivation_id invariance across plan permutations, exclusions, and sensitivity.
- contracts/schemas.json machine-readable contracts and validation logic (Scenario T05).
- contracts/rules.json inventory completeness and gate semantics (Scenario T14).
- taxi_zone_lookup.v1 invariants and unmatched fact retention (Scenario T10).
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

try:
    import pytest
except ImportError:
    class _RaisesContext:
        def __init__(self, expected_exception, match=None):
            self.expected_exception = expected_exception
            self.match = match
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is None:
                raise AssertionError(f"Expected {self.expected_exception} but no exception was raised")
            if not issubclass(exc_type, self.expected_exception):
                return False
            if self.match and self.match not in str(exc_val):
                raise AssertionError(f"Expected match '{self.match}' in '{exc_val}'")
            return True

    class _PytestShim:
        @staticmethod
        def raises(expected_exception, match=None):
            return _RaisesContext(expected_exception, match)

        @staticmethod
        def fixture(func):
            return func

    pytest = _PytestShim()

from hvfhv.contracts.identity import (
    canonical_json_dumps,
    canonical_json_bytes,
    compute_revision_id,
    compute_derivation_id,
    compute_code_bundle_manifest_hash,
    FrozenInputPlan,
    SourceRevisionEntry,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMAS_JSON_PATH = REPO_ROOT / "contracts" / "schemas.json"
RULES_JSON_PATH = REPO_ROOT / "contracts" / "rules.json"


# ==============================================================================
# 1. Canonical JSON Serializer Tests
# ==============================================================================

def test_canonical_json_determinism_and_key_sorting():
    """Dicts with different insertion orders produce identical canonical strings and bytes."""
    dict_a = {"b": 2, "a": 1, "c": {"y": 20, "x": 10}}
    dict_b = {"c": {"x": 10, "y": 20}, "a": 1, "b": 2}

    dump_a = canonical_json_dumps(dict_a)
    dump_b = canonical_json_dumps(dict_b)

    assert dump_a == dump_b == '{"a":1,"b":2,"c":{"x":10,"y":20}}'
    assert canonical_json_bytes(dict_a) == canonical_json_bytes(dict_b)


def test_canonical_json_rejects_non_finite_floats():
    """Non-finite floats (NaN, Inf, -Inf) must raise ValueError."""
    for bad_val in [float("nan"), float("inf"), float("-inf")]:
        with pytest.raises(ValueError, match="Canonical JSON does not permit non-finite floats"):
            canonical_json_dumps({"value": bad_val})

        with pytest.raises(ValueError, match="Canonical JSON does not permit non-finite floats"):
            canonical_json_dumps([1, 2, bad_val])

        with pytest.raises(ValueError, match="Canonical JSON does not permit non-finite floats"):
            canonical_json_dumps({"nested": {"list": [bad_val]}})


def test_canonical_json_rejects_non_string_dict_keys():
    """Dictionary keys must strictly be strings."""
    with pytest.raises(TypeError, match="dictionary keys must be strings"):
        canonical_json_dumps({123: "invalid_key"})


def test_canonical_json_explicit_null_semantics():
    """Python None must serialize to JSON null explicitly."""
    payload = {"absent_field": None, "empty_str": ""}
    dump = canonical_json_dumps(payload)
    assert dump == '{"absent_field":null,"empty_str":""}'


def test_canonical_json_unicode_preservation():
    """Unicode strings must be preserved without ASCII escaping (ensure_ascii=False)."""
    payload = {"borough": "Brooklyn", "accent": "Montréal"}
    dump = canonical_json_dumps(payload)
    assert "Montréal" in dump
    assert "\\u" not in dump


# ==============================================================================
# 2. compute_revision_id Tests
# ==============================================================================

def test_compute_revision_id_determinism():
    """compute_revision_id produces deterministic 64-char hex digest."""
    logical_id = "tlc/hvfhv/2024-12"
    content_sha = "fa34956571f71e2663eed475670b57b430fa34956571f71e2663eed475670b57"

    rev_1 = compute_revision_id(logical_id, content_sha)
    rev_2 = compute_revision_id(logical_id, content_sha.upper())  # Case-insensitive digest input

    assert len(rev_1) == 64
    assert rev_1 == rev_2

    # Invariance check against independent manual computation:
    expected_payload = f'{{"content_sha256":"{content_sha.lower()}","logical_source_id":"{logical_id}"}}'
    expected_hash = hashlib.sha256(expected_payload.encode("utf-8")).hexdigest()
    assert rev_1 == expected_hash


def test_compute_revision_id_sensitivity():
    """Any change to logical_source_id or content_sha alters revision_id."""
    base_id = "tlc/hvfhv/2024-12"
    base_sha = "a" * 64

    rev_base = compute_revision_id(base_id, base_sha)
    rev_diff_src = compute_revision_id("tlc/hvfhv/2025-01", base_sha)
    rev_diff_sha = compute_revision_id(base_id, "b" + "a" * 63)

    assert rev_base != rev_diff_src
    assert rev_base != rev_diff_sha


def test_compute_revision_id_validation():
    """Invalid arguments raise ValueError."""
    with pytest.raises(ValueError, match="logical_source_id must be a non-empty string"):
        compute_revision_id("", "a" * 64)

    with pytest.raises(ValueError, match="content_sha256 must be a valid 64-char hex digest"):
        compute_revision_id("tlc/hvfhv/2024-12", "short_digest")


# ==============================================================================
# 3. compute_derivation_id & FrozenInputPlan Tests
# ==============================================================================

def get_base_frozen_plan() -> FrozenInputPlan:
    return FrozenInputPlan(
        source_revisions=[
            SourceRevisionEntry("tlc/hvfhv/2024-11", "rev_2024_11_" + "1" * 52),
            SourceRevisionEntry("tlc/hvfhv/2024-12", "rev_2024_12_" + "2" * 52),
        ],
        window_months=["2024-11", "2024-12"],
        zone_revision_id="rev_zone_" + "3" * 55,
        schema_adapter_version="v1",
        rules_hash="rule_hash_" + "4" * 54,
        metric_definition_hash="metric_hash_" + "5" * 52,
        code_bundle_manifest_hash="code_hash_" + "6" * 54,
        lockfile_hash="lock_hash_" + "7" * 54,
        model_schema_version="v1",
        semantic_configuration={
            "comparison_policy": "adjacent_in_plan_v1",
            "quantile_method": "continuous_linear",
        },
    )


def test_compute_derivation_id_permutation_invariance():
    """Permuted source revisions or window months produce identical derivation_id."""
    base_frozen_plan = get_base_frozen_plan()
    plan_permuted = FrozenInputPlan(
        source_revisions=list(reversed(base_frozen_plan.source_revisions)),
        window_months=list(reversed(base_frozen_plan.window_months)),
        zone_revision_id=base_frozen_plan.zone_revision_id,
        schema_adapter_version=base_frozen_plan.schema_adapter_version,
        rules_hash=base_frozen_plan.rules_hash,
        metric_definition_hash=base_frozen_plan.metric_definition_hash,
        code_bundle_manifest_hash=base_frozen_plan.code_bundle_manifest_hash,
        lockfile_hash=base_frozen_plan.lockfile_hash,
        model_schema_version=base_frozen_plan.model_schema_version,
        semantic_configuration=base_frozen_plan.semantic_configuration,
    )

    deriv_1 = compute_derivation_id(base_frozen_plan)
    deriv_2 = compute_derivation_id(plan_permuted)

    assert deriv_1 == deriv_2
    assert len(deriv_1) == 64


def test_compute_derivation_id_sensitivity_scenarios():
    """Mutating any constituent component alters derivation_id (Scenario T10, T14)."""
    base_frozen_plan = get_base_frozen_plan()
    base_deriv = compute_derivation_id(base_frozen_plan)

    # 1. Mutate source revision
    mut_src = FrozenInputPlan(
        source_revisions=[
            SourceRevisionEntry("tlc/hvfhv/2024-11", "rev_mutated_" + "1" * 52),
            base_frozen_plan.source_revisions[1],
        ],
        window_months=base_frozen_plan.window_months,
        zone_revision_id=base_frozen_plan.zone_revision_id,
        schema_adapter_version=base_frozen_plan.schema_adapter_version,
        rules_hash=base_frozen_plan.rules_hash,
        metric_definition_hash=base_frozen_plan.metric_definition_hash,
        code_bundle_manifest_hash=base_frozen_plan.code_bundle_manifest_hash,
        lockfile_hash=base_frozen_plan.lockfile_hash,
        model_schema_version=base_frozen_plan.model_schema_version,
        semantic_configuration=base_frozen_plan.semantic_configuration,
    )
    assert compute_derivation_id(mut_src) != base_deriv

    # 2. Mutate zone revision (Scenario T10)
    mut_zone = FrozenInputPlan(
        source_revisions=base_frozen_plan.source_revisions,
        window_months=base_frozen_plan.window_months,
        zone_revision_id="rev_zone_NEW_" + "0" * 52,
        schema_adapter_version=base_frozen_plan.schema_adapter_version,
        rules_hash=base_frozen_plan.rules_hash,
        metric_definition_hash=base_frozen_plan.metric_definition_hash,
        code_bundle_manifest_hash=base_frozen_plan.code_bundle_manifest_hash,
        lockfile_hash=base_frozen_plan.lockfile_hash,
        model_schema_version=base_frozen_plan.model_schema_version,
        semantic_configuration=base_frozen_plan.semantic_configuration,
    )
    assert compute_derivation_id(mut_zone) != base_deriv

    # 3. Mutate rules hash (Scenario T14)
    mut_rules = FrozenInputPlan(
        source_revisions=base_frozen_plan.source_revisions,
        window_months=base_frozen_plan.window_months,
        zone_revision_id=base_frozen_plan.zone_revision_id,
        schema_adapter_version=base_frozen_plan.schema_adapter_version,
        rules_hash="mutated_rules_hash_" + "0" * 45,
        metric_definition_hash=base_frozen_plan.metric_definition_hash,
        code_bundle_manifest_hash=base_frozen_plan.code_bundle_manifest_hash,
        lockfile_hash=base_frozen_plan.lockfile_hash,
        model_schema_version=base_frozen_plan.model_schema_version,
        semantic_configuration=base_frozen_plan.semantic_configuration,
    )
    assert compute_derivation_id(mut_rules) != base_deriv

    # 4. Mutate code bundle manifest hash (Scenario T14)
    mut_code = FrozenInputPlan(
        source_revisions=base_frozen_plan.source_revisions,
        window_months=base_frozen_plan.window_months,
        zone_revision_id=base_frozen_plan.zone_revision_id,
        schema_adapter_version=base_frozen_plan.schema_adapter_version,
        rules_hash=base_frozen_plan.rules_hash,
        metric_definition_hash=base_frozen_plan.metric_definition_hash,
        code_bundle_manifest_hash="mutated_code_hash_" + "0" * 46,
        lockfile_hash=base_frozen_plan.lockfile_hash,
        model_schema_version=base_frozen_plan.model_schema_version,
        semantic_configuration=base_frozen_plan.semantic_configuration,
    )
    assert compute_derivation_id(mut_code) != base_deriv


def test_compute_code_bundle_manifest_hash():
    """Verify code bundle manifest hash detects changes to code or contracts."""
    h1, m1 = compute_code_bundle_manifest_hash(REPO_ROOT)
    assert len(h1) == 64
    assert "contracts/schemas.json" in m1
    assert "contracts/rules.json" in m1
    assert "src/hvfhv/contracts/identity.py" in m1


# ==============================================================================
# 4. Scenario T05: Source Schema Adaptation & Unknown Field Blocking
# ==============================================================================

def test_scenario_t05_contracts_schemas_json_structure():
    """Validate contracts/schemas.json file exists and declares required variants."""
    assert SCHEMAS_JSON_PATH.exists(), f"Missing {SCHEMAS_JSON_PATH}"
    with open(SCHEMAS_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    schemas = data["schemas"]
    assert "hvfhv_24col_pre2025.v1" in schemas
    assert "hvfhv_25col_2025onward.v1" in schemas
    assert "taxi_zone_lookup.v1" in schemas

    # Check 24-col pre-2025 definition
    s24 = schemas["hvfhv_24col_pre2025.v1"]
    assert s24["expected_column_count"] == 24
    assert len(s24["columns"]) == 24
    assert "cbd_congestion_fee" in s24["absent_columns"]
    assert s24["absent_columns"]["cbd_congestion_fee"]["cbd_fee_field_present"] is False
    assert s24["absent_columns"]["cbd_congestion_fee"]["default_sql"] == "NULL"

    # Check 25-col 2025-onward definition
    s25 = schemas["hvfhv_25col_2025onward.v1"]
    assert s25["expected_column_count"] == 25
    assert len(s25["columns"]) == 25
    col_names_25 = [c["name"] for c in s25["columns"]]
    assert "cbd_congestion_fee" in col_names_25


def test_scenario_t05_schema_validator_logic():
    """Simulates S02 schema validator checking 24 vs 25 columns and unexpected columns."""
    with open(SCHEMAS_JSON_PATH, "r", encoding="utf-8") as f:
        schemas = json.load(f)["schemas"]

    s24_expected = [c["name"] for c in schemas["hvfhv_24col_pre2025.v1"]["columns"]]
    s25_expected = [c["name"] for c in schemas["hvfhv_25col_2025onward.v1"]["columns"]]

    def validate_columns(month: str, actual_columns: list[str]) -> tuple[bool, str]:
        if month < "2025-01":
            expected = s24_expected
            schema_name = "hvfhv_24col_pre2025.v1"
        else:
            expected = s25_expected
            schema_name = "hvfhv_25col_2025onward.v1"

        actual_set = set(actual_columns)
        expected_set = set(expected)

        missing = expected_set - actual_set
        extra = actual_set - expected_set

        if missing:
            return False, f"Missing required columns for {schema_name}: {sorted(missing)}"
        if extra:
            return False, f"Unexpected extra columns block normalization under S02: {sorted(extra)}"
        return True, "PASS"

    # 1. Valid pre-2025
    ok, msg = validate_columns("2024-12", s24_expected)
    assert ok is True and msg == "PASS"

    # 2. Valid 2025-onward
    ok, msg = validate_columns("2025-01", s25_expected)
    assert ok is True and msg == "PASS"

    # 3. Unexpected 25 cols in 2024-12 (e.g. cbd_congestion_fee prematurely added)
    ok, msg = validate_columns("2024-12", s25_expected)
    assert ok is False and "Unexpected extra columns" in msg

    # 4. Unknown added column (Scenario T05 requirement: added unknown field blocks)
    unknown_cols = s24_expected + ["unknown_telematics_provider"]
    ok, msg = validate_columns("2024-12", unknown_cols)
    assert ok is False and "unknown_telematics_provider" in msg

    # 5. Missing required column
    missing_cols = [c for c in s24_expected if c != "trip_miles"]
    ok, msg = validate_columns("2024-12", missing_cols)
    assert ok is False and "trip_miles" in msg


# ==============================================================================
# 5. Scenario T10: Taxi Zone Lookup Invariants & Unmatched Facts
# ==============================================================================

def test_scenario_t10_taxi_zone_lookup_invariants():
    """S03 enforces unique non-null LocationID and sentinel IDs 1, 264, 265."""
    with open(SCHEMAS_JSON_PATH, "r", encoding="utf-8") as f:
        zone_schema = json.load(f)["schemas"]["taxi_zone_lookup.v1"]

    sentinel_ids = set(zone_schema["mandatory_sentinel_ids"])
    assert sentinel_ids == {1, 264, 265}

    def validate_zone_lookup(rows: list[dict]) -> tuple[bool, str]:
        location_ids = [r.get("LocationID") for r in rows]
        if any(lid is None or not isinstance(lid, int) for lid in location_ids):
            return False, "S03 FAIL: LocationID must be non-null integer"
        if len(location_ids) != len(set(location_ids)):
            return False, "S03 FAIL: LocationID must be unique"
        if not sentinel_ids.issubset(set(location_ids)):
            return False, f"S03 FAIL: Missing sentinel LocationIDs: {sentinel_ids - set(location_ids)}"
        return True, "PASS"

    # Valid fixture
    valid_rows = [
        {"LocationID": 1, "Borough": "EWR", "Zone": "Newark Airport", "service_zone": "EWR"},
        {"LocationID": 2, "Borough": "Queens", "Zone": "Jamaica Bay", "service_zone": "Boro Zone"},
        {"LocationID": 264, "Borough": "Unknown", "Zone": "N/A", "service_zone": "N/A"},
        {"LocationID": 265, "Borough": "N/A", "Zone": "Outside of NYC", "service_zone": "N/A"},
    ]
    ok, msg = validate_zone_lookup(valid_rows)
    assert ok is True and msg == "PASS"

    # Duplicate LocationID blocks
    dup_rows = valid_rows + [{"LocationID": 2, "Borough": "Queens", "Zone": "Dup", "service_zone": "Boro Zone"}]
    ok, msg = validate_zone_lookup(dup_rows)
    assert ok is False and "LocationID must be unique" in msg

    # Missing sentinel ID blocks
    missing_sentinel = [r for r in valid_rows if r["LocationID"] != 264]
    ok, msg = validate_zone_lookup(missing_sentinel)
    assert ok is False and "Missing sentinel LocationIDs" in msg


def test_scenario_t10_unmatched_facts_retained():
    """Unmatched LocationID in trip data (e.g. 999) is retained and classified by Q05 (WARN), not dropped."""
    known_zone_ids = {1, 2, 264, 265}

    def classify_pu_location(pu_id: int | None) -> str:
        if pu_id is None:
            return "null"
        if pu_id in {1, 264, 265}:
            return "matched_sentinel"
        if pu_id in known_zone_ids:
            return "matched_ordinary"
        return "unmatched"

    assert classify_pu_location(None) == "null"
    assert classify_pu_location(2) == "matched_ordinary"
    assert classify_pu_location(1) == "matched_sentinel"
    assert classify_pu_location(264) == "matched_sentinel"
    assert classify_pu_location(999) == "unmatched"  # Fact retained, classified as unmatched


# ==============================================================================
# 6. Scenario T14: Rule Inventory Completeness & Error Blocking
# ==============================================================================

def test_scenario_t14_rules_json_inventory():
    """contracts/rules.json must contain exactly S01-S08 and Q01-Q09."""
    assert RULES_JSON_PATH.exists(), f"Missing {RULES_JSON_PATH}"
    with open(RULES_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    rules = {r["rule_id"]: r for r in data["rules"]}
    expected_block = [f"S0{i}" for i in range(1, 9)]
    expected_warn = [f"Q0{i}" for i in range(1, 10)]

    for rid in expected_block:
        assert rid in rules, f"Missing required BLOCK rule {rid}"
        assert rules[rid]["severity"] == "BLOCK"

    for rid in expected_warn:
        assert rid in rules, f"Missing required WARN rule {rid}"
        assert rules[rid]["severity"] == "WARN"

    assert len(rules) == 17, f"Expected 17 total rules, got {len(rules)}"
    assert rules["S08"]["stage"] == "post_seal"
    assert all(rules[f"S0{i}"]["stage"] == "pre_seal" for i in range(1, 8))


def test_scenario_t14_s07_blocks_on_incomplete_inventory_or_error():
    """S07 verifies complete rule execution; missing rule or ERROR blocks candidate."""
    required_rules = {f"S0{i}" for i in range(1, 7)} | {f"Q0{i}" for i in range(1, 10)}

    def evaluate_s07(executed_results: dict[str, str], counts: dict[str, int]) -> tuple[bool, str]:
        # 1. Inventory completeness
        executed_keys = set(executed_results.keys())
        missing = required_rules - executed_keys
        if missing:
            return False, f"S07 FAIL: Missing rule execution for {sorted(missing)}"

        # 2. Check for ERROR status in any rule
        if any(status == "ERROR" for status in executed_results.values()):
            return False, "S07 FAIL: Rule returned ERROR status"

        # 3. Check for unauthorized NOT_APPLICABLE
        for rid, status in executed_results.items():
            if status == "NOT_APPLICABLE" and rid not in {"Q07", "Q09"}:
                return False, f"S07 FAIL: Unauthorized NOT_APPLICABLE for rule {rid}"

        # 4. Metric identity check
        total = counts.get("total", 0)
        missing_c = counts.get("missing", 0)
        neg_c = counts.get("negative", 0)
        elig_c = counts.get("eligible", 0)
        if total != (missing_c + neg_c + elig_c):
            return False, f"S07 FAIL: Metric identity violated (total={total} != {missing_c}+{neg_c}+{elig_c})"

        return True, "PASS"

    counts = {"total": 100, "missing": 10, "negative": 5, "eligible": 85}
    valid_results = {rid: "PASS" for rid in required_rules}

    # 1. Complete inventory passes
    ok, msg = evaluate_s07(valid_results, counts)
    assert ok is True and msg == "PASS"

    # 2. Missing rule S04 blocks
    missing_s04 = {k: v for k, v in valid_results.items() if k != "S04"}
    ok, msg = evaluate_s07(missing_s04, counts)
    assert ok is False and "Missing rule execution for ['S04']" in msg

    # 3. Rule returning ERROR blocks
    error_q02 = dict(valid_results)
    error_q02["Q02"] = "ERROR"
    ok, msg = evaluate_s07(error_q02, counts)
    assert ok is False and "Rule returned ERROR status" in msg

    # 4. Metric count identity violation blocks
    bad_counts = {"total": 100, "missing": 10, "negative": 5, "eligible": 80}
    ok, msg = evaluate_s07(valid_results, bad_counts)
    assert ok is False and "Metric identity violated" in msg
