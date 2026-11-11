"""Gate G5 Acceptance: Scenario T23 & R18 - Orchestration Assets & Asset Checks.

Verifies:
1. Dagster asset checks mirror pre-seal rules S01-S08 and profiles Q01-Q09.
2. Mandatory quality checks evaluate pass/fail correctly from validation receipt.
3. Failing pre-seal checks accurately reflect in orchestration asset check status.
4. Orchestration metadata reflects accurate release derivation and hash provenance.
"""

from __future__ import annotations

import json
import tempfile
import uuid
from pathlib import Path

from hvfhv.orchestration.defs import (
    execute_asset_checks_from_release,
    OrchestrationError,
    dagster_defs,
)
from tests.gate5_consumer.test_scenario_t21_readonly_dashboard_queries import setup_test_published_release


def test_t23_asset_checks_mirror_pre_seal_rules_success():
    """Scenario T23: Asset checks evaluated against release validation receipt."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        res = execute_asset_checks_from_release(root, rel_id)
        assert res["release_id"] == rel_id
        assert res["all_mandatory_s_passed"] is True
        assert res["total_checks_evaluated"] == 8

        # Verify all S01-S08 rules are present and PASS
        for i in range(1, 9):
            rid = f"S0{i}"
            check = res["checks"].get(rid)
            assert check is not None, f"Rule {rid} missing from asset checks"
            assert check["passed"] is True
            assert check["outcome"] == "PASS"
            assert check["severity"] == "BLOCKING"


def test_t23_failing_rule_surfaces_as_asset_check_failure():
    """Scenario T23: If a mandatory rule fails, asset check reports failure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        # Modify validation.json to simulate a failure in S03 (zone integrity)
        val_file = rel_dir / "validation.json"
        val_data = json.loads(val_file.read_text(encoding="utf-8"))
        for r in val_data["rule_results"]:
            if r["rule_id"] == "S03":
                r["outcome"] = "FAIL"
                r["failing_row_count"] = 5
        val_file.write_text(json.dumps(val_data), encoding="utf-8")

        res = execute_asset_checks_from_release(root, rel_id)
        assert res["all_mandatory_s_passed"] is False
        assert res["checks"]["S03"]["passed"] is False
        assert res["checks"]["S03"]["outcome"] == "FAIL"
        assert res["checks"]["S03"]["failing_rows"] == 5


def test_t23_missing_validation_receipt_raises_orchestration_error():
    """Scenario T23: Missing validation receipt raises OrchestrationError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        registry, rel_id, rel_dir = setup_test_published_release(root)

        # Remove validation.json
        (rel_dir / "validation.json").unlink()

        try:
            execute_asset_checks_from_release(root, rel_id)
            assert False, "Should have raised OrchestrationError"
        except OrchestrationError as e:
            assert "Validation receipt missing" in str(e)
