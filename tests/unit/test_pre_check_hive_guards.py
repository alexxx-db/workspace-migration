"""Unit tests for the like-for-like Hive pre_check guard helpers."""

from __future__ import annotations

from pre_check.pre_check import missing_required_mounts


class TestMissingRequiredMounts:
    def test_flags_absent_mounts(self):
        target = ["/mnt/present", "/mnt/other"]
        required = ["present", "salesraw"]
        assert missing_required_mounts(target, required) == ["salesraw"]

    def test_all_present(self):
        assert missing_required_mounts(["/mnt/a", "/mnt/b"], ["a", "b"]) == []

    def test_handles_bare_and_prefixed_target_forms(self):
        # Target mounts may come back as "/mnt/x" or "x" depending on SDK shape.
        assert missing_required_mounts(["x"], ["x"]) == []
        assert missing_required_mounts(["/mnt/x/"], ["x"]) == []

    def test_empty_required_is_no_missing(self):
        assert missing_required_mounts([], []) == []


class TestPreCheckReferencesStagingKey:
    def test_source_uses_staging_path_not_target_path(self):
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "pre_check" / "pre_check.py").read_text()
        assert "hive_dbfs_staging_path" in src
        assert "hive_dbfs_target_path" not in src
        assert "check_target_mounts" in src
        assert "check_staging_path_reachable" in src


class TestTargetDbfsRootProbeVerdict:
    """The DBFS-root two-hop creates MANAGED tables in the TARGET's own DBFS
    root (via ``CREATE DATABASE``/``CREATE TABLE`` in the target hive_metastore).
    The guard must probe the TARGET — its earlier form wrote via ``dbutils.fs``,
    which hits the SOURCE (deploy) workspace, giving a false PASS when the
    target's DBFS root is disabled. ``target_dbfs_root_probe_verdict`` turns a
    target-warehouse result into a PASS/FAIL decision, keyed off the real
    DBFS_DISABLED signature the orchestrator hits."""

    def test_succeeded_is_pass(self):
        from pre_check.pre_check import target_dbfs_root_probe_verdict

        status, _ = target_dbfs_root_probe_verdict({"state": "SUCCEEDED"})
        assert status == "PASS"

    def test_dbfs_disabled_error_is_fail(self):
        from pre_check.pre_check import target_dbfs_root_probe_verdict

        res = {
            "state": "FAILED",
            "error": "[DBFS_DISABLED] Public DBFS root is disabled. Access is denied "
            "on path: /user/hive/warehouse/x.db SQLSTATE: 56038",
        }
        status, msg = target_dbfs_root_probe_verdict(res)
        assert status == "FAIL"
        assert "dbfs" in msg.lower()

    def test_other_failure_is_warn_not_false_pass(self):
        from pre_check.pre_check import target_dbfs_root_probe_verdict

        # An unrelated failure (e.g. transient warehouse error) must NOT be
        # reported as PASS — degrade to WARN so it doesn't mask the real state.
        status, _ = target_dbfs_root_probe_verdict({"state": "FAILED", "error": "boom"})
        assert status == "WARN"


class TestTargetDbfsRootProbesTargetNotSource:
    def test_check_no_longer_uses_dbutils_fs_for_dbfs_root(self):
        """Regression for the false-PASS: the guard must NOT probe DBFS-root
        via dbutils.fs (that runs on the SOURCE/deploy workspace). It must use
        the target warehouse (execute_and_poll on the target)."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "pre_check" / "pre_check.py").read_text()
        # Isolate the check_target_dbfs_root block.
        start = src.index("check_target_dbfs_root")
        block = src[start : start + 1500]
        assert "dbutils.fs.put" not in block, (
            "check_target_dbfs_root must not probe via dbutils.fs (that hits the "
            "SOURCE workspace) — probe the TARGET via the warehouse."
        )
        assert "target_dbfs_root_probe_verdict" in block
        assert "execute_and_poll" in block


class TestTargetWarehouseDacProbeVerdict:
    """Finding #25: the DBFS-root two-hop STAGE-2 and ADLS-backed Hive migration
    write/read ``abfss://`` paths through the TARGET SQL warehouse, which needs a
    Data Access Config (``fs.azure.account.key`` / OAuth). ``check_target_dbfs_root``
    only created a *database* in the target's DEFAULT DBFS root — no ``abfss``, no
    account key — so an empty DAC passed green and the migration then died mid-run
    on ``fs.azure.account.key``. ``target_warehouse_dac_probe_verdict`` turns a
    target-warehouse abfss create-probe result into a PASS/FAIL decision keyed off
    the real missing-DAC signature."""

    def test_succeeded_is_pass(self):
        from pre_check.pre_check import target_warehouse_dac_probe_verdict

        status, _ = target_warehouse_dac_probe_verdict({"state": "SUCCEEDED"})
        assert status == "PASS"

    def test_missing_account_key_is_fail(self):
        from pre_check.pre_check import target_warehouse_dac_probe_verdict

        res = {
            "state": "FAILED",
            "error": "Failure to initialize configuration for storage account "
            "stextsourcemig36cd38.dfs.core.windows.net: Invalid configuration value "
            "detected for fs.azure.account.key",
        }
        status, msg = target_warehouse_dac_probe_verdict(res)
        assert status == "FAIL"
        assert "data access" in msg.lower() or "account key" in msg.lower()

    def test_other_failure_is_warn_not_false_pass(self):
        from pre_check.pre_check import target_warehouse_dac_probe_verdict

        # An unrelated failure must NOT be reported as PASS — degrade to WARN so
        # it doesn't mask the real missing-DAC state.
        status, _ = target_warehouse_dac_probe_verdict({"state": "FAILED", "error": "boom"})
        assert status == "WARN"


class TestTargetWarehouseDacCheckWired:
    def test_check_probes_target_abfss_via_warehouse(self):
        """The new #25 guard must actually exercise the DAC: create a table at an
        ``abfss://`` LOCATION under the staging path on the TARGET warehouse
        (execute_and_poll), gated on migrate_hive_dbfs_root, mapping via the
        verdict helper. A DEFAULT-DBFS-root database create does NOT touch the
        account key and cannot catch the gap."""
        import pathlib

        src = (pathlib.Path(__file__).resolve().parents[2] / "src" / "pre_check" / "pre_check.py").read_text()
        assert "check_target_warehouse_dac" in src
        start = src.index("check_target_warehouse_dac")
        block = src[start : start + 1600]
        assert "target_warehouse_dac_probe_verdict" in block
        assert "execute_and_poll" in block
        assert "abfss://" in block
        assert "migrate_hive_dbfs_root" in block
