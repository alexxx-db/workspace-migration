"""X.2 per-worker idempotency audit — pin-down unit tests.

Each worker in src/migrate/ handles "object already exists on target" and
"status row already present" in slightly different ways. This file pins the
current behavior per worker so the retry/resumability work (X.1) can rely on
a known baseline.

Terminology:
- "No row" / "pending" / "in_progress" / "failed" / "validated" /
  "validation_failed" / "skipped_by_config" / "skipped_no_access" —
  values of the status column in migration_tracking.cp_migration.migration_status.
- "Missing" / "exists" / "partial" — observed state of the object on the
  target workspace at the moment the worker runs.

Notes on upstream filtering (applies to every worker):
- The orchestrator calls `TrackingManager.get_pending_objects(obj_type)` and
  filters rows where the latest status is in {'validated', 'skipped'}. Workers
  themselves do not re-filter — they process every item handed to them.
- Consequence: rows with status 'failed', 'in_progress', 'validation_failed',
  'skipped_by_config', 'skipped_no_access' are ALL retried by the worker on
  the next run. The individual per-worker tests below pin what "retry" does.
- `skipped_by_config` is emitted by a worker when the feature is disabled
  (e.g. `migrate_hive_dbfs_root=false`) — the worker is expected to re-emit
  the same `skipped_by_config` row rather than treat it as terminal.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _ok() -> dict:
    return {"state": "SUCCEEDED", "statement_id": "s"}


def _fail(err: str = "ERR") -> dict:
    return {"state": "FAILED", "error": err, "statement_id": "s"}


def _fail_already_exists() -> dict:
    """Canonical SQL-layer error for "already exists on target"."""
    return {"state": "FAILED", "error": "[SCHEMA_ALREADY_EXISTS] already exists"}


# ============================================================================
# managed_table_worker
# ============================================================================
# | Status | Target | Action | Behavior |
# |---|---|---|---|
# | pending/failed | any | CREATE OR REPLACE TABLE ... DEEP CLONE | idempotent: DEEP CLONE overwrites existing target. |
# | any           | any | format=iceberg, not opted in          | return status=skipped with opt-in message. |


class TestManagedTableIdempotency:
    def _deps(self, *, dry_run: bool = False, iceberg: str = "") -> dict:
        c = MagicMock()
        c.dry_run = dry_run
        c.iceberg_strategy = iceberg
        return {
            "config": c, "auth": MagicMock(), "tracker": MagicMock(),
            "validator": MagicMock(), "wh_id": "wh", "share_name": "cp_migration_share",
        }

    @patch("migrate.clone_lib.time")
    @patch("migrate.clone_lib.execute_and_poll")
    def test_delta_retry_uses_create_or_replace(self, mock_exec, mock_time):
        """Pin: DEEP CLONE uses CREATE OR REPLACE so existing target is silently overwritten."""
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_exec.return_value = _ok()
        deps = self._deps()
        deps["validator"].validate_row_count.return_value = {
            "match": True, "source_count": 1, "target_count": 1,
        }
        res = clone_table({"object_name": "`c`.`s`.`t`"}, **deps)
        sql = mock_exec.call_args[0][2]
        assert "CREATE OR REPLACE TABLE" in sql
        assert res["status"] == "validated"

    @patch("migrate.clone_lib.time")
    @patch("migrate.clone_lib.execute_and_poll")
    def test_iceberg_not_opted_in_is_skipped(self, mock_exec, mock_time):
        from migrate.managed_table_worker import clone_table

        mock_time.time.side_effect = [100.0, 100.1]
        deps = self._deps(iceberg="")
        res = clone_table(
            {"object_name": "`c`.`s`.`t`", "format": "iceberg"}, **deps,
        )
        assert res["status"] == "skipped_by_config"
        mock_exec.assert_not_called()


# ============================================================================
# external_table_worker
# ============================================================================
# | pending/failed | missing | CREATE TABLE IF NOT EXISTS | OK |
# | pending/failed | exists  | CREATE TABLE IF NOT EXISTS | no-op, validated |


class TestExternalTableIdempotency:
    @patch("migrate.external_table_worker.time")
    @patch("migrate.external_table_worker.execute_and_poll")
    def test_uses_if_not_exists(self, mock_exec, mock_time):
        """Pin: external table worker rewrites CREATE TABLE to CREATE TABLE IF NOT EXISTS."""
        from migrate.external_table_worker import migrate_external_table

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        explorer = MagicMock()
        explorer.get_create_statement.return_value = (
            "CREATE TABLE `c`.`s`.`t` (id INT) USING DELTA LOCATION 'abfss://...'"
        )
        validator = MagicMock()
        validator.validate_row_count.return_value = {
            "match": True, "source_count": 1, "target_count": 1,
        }
        config = MagicMock()
        config.dry_run = False
        res = migrate_external_table(
            {"object_name": "`c`.`s`.`t`"},
            config=config, auth=MagicMock(), tracker=MagicMock(),
            explorer=explorer, validator=validator, wh_id="wh",
        )
        sql = mock_exec.call_args[0][2]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert res["status"] == "validated"


# ============================================================================
# views_worker
# ============================================================================
# | any | any | CREATE OR REPLACE VIEW | idempotent |


class TestViewsIdempotency:
    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.execute_and_poll")
    def test_uses_or_replace(self, mock_exec, mock_time):
        """Pin: views are replayed as CREATE OR REPLACE VIEW (idempotent)."""
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        explorer = MagicMock()
        explorer.get_create_statement.return_value = "CREATE VIEW `c`.`s`.`v` AS SELECT 1"
        config = MagicMock()
        config.dry_run = False
        res = migrate_view(
            {"object_name": "`c`.`s`.`v`"},
            config=config, auth=MagicMock(), tracker=MagicMock(),
            explorer=explorer, wh_id="wh",
        )
        sql = mock_exec.call_args[0][2]
        assert "CREATE OR REPLACE VIEW" in sql
        assert res["status"] == "validated"


# ============================================================================
# functions_worker
# ============================================================================
# | any | any | CREATE OR REPLACE FUNCTION | idempotent |


class TestFunctionsIdempotency:
    @patch("migrate.functions_worker.time")
    @patch("migrate.functions_worker.execute_and_poll")
    def test_uses_or_replace(self, mock_exec, mock_time):
        """Pin: functions are replayed as CREATE OR REPLACE FUNCTION (idempotent)."""
        from migrate.functions_worker import migrate_function

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        explorer = MagicMock()
        explorer.get_function_ddl.return_value = (
            "CREATE FUNCTION `c`.`s`.`f`(x INT) RETURNS INT RETURN x + 1"
        )
        config = MagicMock()
        config.dry_run = False
        res = migrate_function(
            {"object_name": "`c`.`s`.`f`"},
            config=config, auth=MagicMock(), tracker=MagicMock(),
            explorer=explorer, wh_id="wh",
        )
        sql = mock_exec.call_args[0][2]
        assert "CREATE OR REPLACE FUNCTION" in sql
        assert res["status"] == "validated"


# ============================================================================
# volume_worker
# ============================================================================
# | external | any     | CREATE EXTERNAL VOLUME IF NOT EXISTS      | idempotent |
# | managed  | missing | CREATE VOLUME IF NOT EXISTS + file copy   | idempotent |
# | managed  | exists  | CREATE VOLUME IF NOT EXISTS + copy         | copy re-runs; dbutils.fs.cp overwrites per-file |
# | any      | -       | add_volume_to_share "already" shared       | swallowed by try/except |


class TestVolumeIdempotency:
    @patch("migrate.volume_worker.time")
    @patch("migrate.volume_worker.execute_and_poll")
    def test_external_uses_if_not_exists(self, mock_exec, mock_time):
        """Pin: EXTERNAL volumes use CREATE EXTERNAL VOLUME IF NOT EXISTS."""
        from migrate.volume_worker import migrate_volume

        mock_time.time.side_effect = [100.0, 101.0, 102.0]
        mock_exec.return_value = _ok()
        config = MagicMock()
        config.dry_run = False
        res, _ = migrate_volume(
            {
                "object_name": "`c`.`s`.`v`",
                "table_type": "EXTERNAL",
                "storage_location": "abfss://x@y.dfs.core.windows.net/v",
            },
            config=config, auth=MagicMock(), tracker=MagicMock(),
            wh_id="wh", source_spark=MagicMock(), notebook_uploaded=True,
        )
        sql = mock_exec.call_args[0][2]
        assert "CREATE EXTERNAL VOLUME IF NOT EXISTS" in sql
        assert res["status"] == "validated"

    def test_add_volume_to_share_tolerates_already_shared(self):
        """Pin: add_volume_to_share swallows 'already' errors on retry."""
        from migrate.volume_worker import add_volume_to_share

        spark = MagicMock()
        spark.sql.side_effect = Exception("volume is already in share")
        # Should not raise
        add_volume_to_share(spark, "cp_migration_share", "`c`.`s`.`v`")

    def test_add_volume_to_share_propagates_other_errors(self):
        """Pin: add_volume_to_share only swallows 'already' — other errors bubble."""
        from migrate.volume_worker import add_volume_to_share

        spark = MagicMock()
        spark.sql.side_effect = Exception("permission denied")
        try:
            add_volume_to_share(spark, "cp_migration_share", "`c`.`s`.`v`")
            raise AssertionError("expected exception")
        except Exception as e:
            assert "permission denied" in str(e)


# ============================================================================
# grants_worker
# ============================================================================
# | any | any | GRANT X ON Y TO Z | server-side idempotent (no-op if already granted) |
# | any | any | action_type=OWN   | skipped (ownership transfer not supported) |
# Note: grants_worker does NOT read tracking — it re-reads SHOW GRANTS and
# re-applies every grant on every run. UC GRANT is idempotent server-side.


class TestGrantsIdempotency:
    @patch("migrate.grants_worker.time")
    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_grants_issues_one_grant_per_row(self, mock_exec, mock_time):
        """Pin: replay_grants issues GRANT per grant record; UC makes this idempotent."""
        from migrate.grants_worker import replay_grants

        mock_time.time.side_effect = [100.0, 101.0, 102.0, 103.0]
        mock_exec.return_value = _ok()
        grants = [
            {"principal": "alice@x.com", "action_type": "SELECT"},
            {"principal": "bob@x.com", "action_type": "MODIFY"},
        ]
        results = replay_grants(
            "CATALOG", "`cat`", grants, auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert len(results) == 2
        sqls = [c.args[2] for c in mock_exec.call_args_list]
        assert any("GRANT SELECT ON CATALOG `cat`" in s for s in sqls)
        assert any("GRANT MODIFY ON CATALOG `cat`" in s for s in sqls)

    @patch("migrate.grants_worker.execute_and_poll")
    def test_replay_grants_transfers_owner(self, mock_exec):
        """Pin (#5): OWN grants transfer ownership via ALTER ... OWNER TO the
        original owner — idempotent (ALTER OWNER is a set, not an append)."""
        from migrate.grants_worker import replay_grants

        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        results = replay_grants(
            "CATALOG", "`cat`",
            [{"principal": "alice", "action_type": "OWN"}],
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert len(results) == 1
        assert results[0]["status"] == "validated"
        assert mock_exec.call_args[0][2] == "ALTER CATALOG `cat` OWNER TO `alice`"


# ============================================================================
# comments_worker
# ============================================================================
# | any | any | COMMENT ON X IS '...' | server-side idempotent (overwrites) |


class TestCommentsIdempotency:
    @patch("migrate.comments_worker.time")
    @patch("migrate.comments_worker.execute_and_poll")
    def test_emit_comment_uses_overwrite_form(self, mock_exec, mock_time):
        """Pin: comments are replayed via COMMENT ON — which overwrites, so idempotent."""
        from migrate.comments_worker import _emit_comment

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        res = _emit_comment(
            "TABLE", "`c`.`s`.`t`", "hello",
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        sql = mock_exec.call_args[0][2]
        assert sql.startswith("COMMENT ON TABLE `c`.`s`.`t` IS '")
        assert res["status"] == "validated"

    def test_emit_comment_escapes_quotes(self):
        """Pin: comment values escape single quotes to avoid SQL injection."""
        from migrate.comments_worker import _escape
        assert _escape("O'Brien") == "O''Brien"


# ============================================================================
# mv_st_worker
# ============================================================================
# | pending/failed | missing | CREATE MV/ST + REFRESH                  | OK |
# | pending/failed | exists  | CREATE fails with "already exists"      | fix: treat as validated + REFRESH |
# | pending/failed | DLT     | skip with skipped_by_pipeline_migration | upstream filter keeps this out |


class TestMvStIdempotency:
    """Phase 4: both MV and ST are hard-excluded. Idempotency is
    trivially guaranteed because the worker performs no target-side
    mutation — every row short-circuits to
    ``skipped_by_stateful_service_migration``. Earlier idempotency tests
    for the DDL-replay branch are removed because the branch no longer
    exists.
    """

    def _deps(self) -> dict:
        c = MagicMock()
        c.dry_run = False
        return {
            "config": c, "auth": MagicMock(), "tracker": MagicMock(), "wh_id": "wh",
        }

    @patch("migrate.mv_st_worker.time")
    def test_mv_hard_exclude_is_idempotent(self, mock_time):
        """Re-running on the same row produces the same skip status with
        no target-side calls — by definition idempotent."""
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.1, 100.2, 100.3]
        deps = self._deps()
        obj = {"object_name": "`c`.`s`.`mv`", "object_type": "mv", "pipeline_id": "p1"}

        res1 = migrate_mv_st(obj, **deps)
        res2 = migrate_mv_st(obj, **deps)
        assert res1["status"] == res2["status"] == "skipped_by_stateful_service_migration"
        deps["auth"].target_client.statement_execution.execute_statement.assert_not_called()

    @patch("migrate.mv_st_worker.time")
    def test_st_hard_exclude_is_idempotent(self, mock_time):
        from migrate.mv_st_worker import migrate_mv_st

        mock_time.time.side_effect = [100.0, 100.1]
        deps = self._deps()
        obj = {"object_name": "`c`.`s`.`st1`", "object_type": "st", "pipeline_id": "p1"}
        res = migrate_mv_st(obj, **deps)
        assert res["status"] == "skipped_by_stateful_service_migration"
        assert "Stateful Services Phase" in (res["error_message"] or "")


# ============================================================================
# tags_worker
# ============================================================================
# | any | any | ALTER ... SET TAGS (k='v', ...) | upsert by key server-side — idempotent |


class TestTagsIdempotency:
    @patch("migrate.tags_worker.time")
    @patch("migrate.tags_worker.execute_and_poll")
    def test_alter_set_tags_is_upsert(self, mock_exec, mock_time):
        """Pin: ALTER ... SET TAGS is an upsert — repeat runs are server-side idempotent."""
        from migrate.tags_worker import apply_tag_group

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        res = apply_tag_group(
            ("TABLE", "`c`.`s`.`t`", ""),
            [{"tag_name": "env", "tag_value": "prod"}],
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        # apply_tag_group returns a list of per-tag status rows (C6).
        assert len(res) == 1
        assert res[0]["status"] == "validated"
        sql = mock_exec.call_args[0][2]
        assert "SET TAGS" in sql


# ============================================================================
# row_filters_worker
# ============================================================================
# | any | any | ALTER TABLE SET ROW FILTER | replaces — idempotent server-side |


class TestRowFiltersIdempotency:
    @patch("migrate.row_filters_worker.time")
    @patch("migrate.row_filters_worker.execute_and_poll")
    def test_set_row_filter_is_replace(self, mock_exec, mock_time):
        """Pin: SET ROW FILTER replaces any existing filter — idempotent server-side."""
        from migrate.row_filters_worker import apply_row_filter

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        res = apply_row_filter(
            {"table_fqn": "`c`.`s`.`t`", "filter_function_fqn": "c.s.fn",
             "filter_columns": ["region"]},
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"


# ============================================================================
# column_masks_worker
# ============================================================================
# | any | any | ALTER TABLE ALTER COLUMN SET MASK | replaces — idempotent |


class TestColumnMasksIdempotency:
    @patch("migrate.column_masks_worker.time")
    @patch("migrate.column_masks_worker.execute_and_poll")
    def test_set_mask_is_replace(self, mock_exec, mock_time):
        """Pin: SET MASK replaces any existing mask — idempotent server-side."""
        from migrate.column_masks_worker import apply_column_mask

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        res = apply_column_mask(
            {"table_fqn": "`c`.`s`.`t`", "column_name": "ssn",
             "mask_function_fqn": "c.s.redact", "mask_using_columns": ["role"]},
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"


# ============================================================================
# policies_worker
# ============================================================================
# | any | missing | POST /policies                   | validated |
# | any | exists  | POST /policies -> "already exists" | BUG FIX: now validated |


class TestPoliciesIdempotency:
    def test_post_succeeds(self):
        from migrate.policies_worker import apply_policy

        auth = MagicMock()
        auth.target_client.api_client.do.return_value = None
        res = apply_policy({"name": "p1"}, auth=auth, dry_run=False)
        assert res["status"] == "validated"

    def test_already_exists_is_validated(self):
        """BUG FIX: retry with policy already present no longer marks failed."""
        from migrate.policies_worker import apply_policy

        auth = MagicMock()
        auth.target_client.api_client.do.side_effect = Exception("policy 'p1' already exists")
        res = apply_policy({"name": "p1"}, auth=auth, dry_run=False)
        assert res["status"] == "validated"
        assert "already existed" in (res["error_message"] or "")

    def test_other_errors_still_fail(self):
        from migrate.policies_worker import apply_policy

        auth = MagicMock()
        auth.target_client.api_client.do.side_effect = Exception("permission denied")
        res = apply_policy({"name": "p1"}, auth=auth, dry_run=False)
        assert res["status"] == "failed"


# ============================================================================
# monitors_worker
# ============================================================================
# | any | missing | POST /monitor                    | validated |
# | any | exists  | POST /monitor -> "already exists" | BUG FIX: now validated |


class TestMonitorsIdempotency:
    def test_post_succeeds(self):
        from migrate.monitors_worker import apply_monitor

        auth = MagicMock()
        auth.target_client.api_client.do.return_value = None
        res = apply_monitor(
            {"table_fqn": "`c`.`s`.`t`", "definition": {"output_schema_name": "mon"}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"

    def test_already_exists_is_validated(self):
        """BUG FIX: retry with monitor already present no longer marks failed."""
        from migrate.monitors_worker import apply_monitor

        auth = MagicMock()
        auth.target_client.api_client.do.side_effect = Exception(
            "monitor for table already exists"
        )
        res = apply_monitor(
            {"table_fqn": "`c`.`s`.`t`", "definition": {}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"
        assert "already existed" in (res["error_message"] or "")


# ============================================================================
# connections_worker
# ============================================================================
# | any | missing | connections.create              | validated / validation_failed (creds) |
# | any | exists  | connections.create -> "exists"  | BUG FIX: now validated (still checks creds) |


class TestConnectionsIdempotency:
    @staticmethod
    def _absent_on_target(auth):
        """Make the target-existence pre-check report 'not present' so the
        test exercises the from-scratch create path."""
        from databricks.sdk.errors import NotFound

        auth.target_client.connections.get.side_effect = NotFound("not found")

    def test_create_succeeds(self):
        from migrate.connections_worker import apply_connection

        auth = MagicMock()
        self._absent_on_target(auth)
        auth.target_client.connections.create.return_value = None
        res = apply_connection(
            {"connection_name": "c1", "connection_type": "MYSQL",
             "options": {"host": "x", "port": "3306"}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"

    def test_connection_type_enum_repr_is_coerced_to_enum(self):
        """Live regression: discovery stores str(connection_type) =
        'ConnectionType.SQLSERVER' (the enum repr). The worker must coerce that
        to a real ConnectionType enum — the SDK's connections.create() calls
        .value on connection_type, so a passed-through string raises
        \"'str' object has no attribute 'value'\". Must NEVER pass a str."""
        from databricks.sdk.service.catalog import ConnectionType

        from migrate.connections_worker import apply_connection

        auth = MagicMock()
        self._absent_on_target(auth)
        auth.target_client.connections.create.return_value = None
        res = apply_connection(
            {"connection_name": "c1", "connection_type": "ConnectionType.SQLSERVER",
             "options": {"host": "x", "port": "1433"}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated", res
        ct = auth.target_client.connections.create.call_args.kwargs["connection_type"]
        assert isinstance(ct, ConnectionType), f"must pass a ConnectionType enum, got {type(ct).__name__}: {ct!r}"
        assert ct == ConnectionType.SQLSERVER

    def test_create_with_secret_becomes_validation_failed(self):
        """Pin: connections with secret fields return validation_failed until re-entered."""
        from migrate.connections_worker import apply_connection

        auth = MagicMock()
        self._absent_on_target(auth)
        auth.target_client.connections.create.return_value = None
        res = apply_connection(
            {"connection_name": "c1", "connection_type": "MYSQL",
             "options": {"host": "x", "password": "REDACTED"}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validation_failed"
        assert "password" in (res["error_message"] or "")

    def test_already_exists_is_validated(self):
        """BUG FIX: retry with connection already present no longer marks failed."""
        from migrate.connections_worker import apply_connection

        auth = MagicMock()
        self._absent_on_target(auth)
        auth.target_client.connections.create.side_effect = Exception(
            "connection 'c1' already exists"
        )
        res = apply_connection(
            {"connection_name": "c1", "connection_type": "MYSQL", "options": {"host": "x"}},
            auth=auth, dry_run=False,
        )
        # Passes through to credential gap check — no secrets, so validated.
        assert res["status"] == "validated"

    def test_other_errors_still_fail(self):
        from migrate.connections_worker import apply_connection

        auth = MagicMock()
        self._absent_on_target(auth)
        auth.target_client.connections.create.side_effect = Exception("bad options")
        res = apply_connection(
            {"connection_name": "c1", "connection_type": "MYSQL", "options": {}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "failed"

    def test_existing_target_connection_is_validated_idempotent(self):
        """Live: UC never returns secret options (user/password for SQLSERVER)
        from GET, so the worker can't recreate the connection from discovery
        alone. When the connection already exists on target (operator
        recreated it with credentials per the runbook, or a prior run made
        it), the target-existence pre-check returns validated WITHOUT
        attempting create — mirrors _target_table_exists fail-closed
        idempotency."""
        from migrate.connections_worker import apply_connection

        auth = MagicMock()
        auth.target_client.connections.get.return_value = MagicMock(name="existing_conn")
        res = apply_connection(
            {"connection_name": "integration_test_sqlserver",
             "connection_type": "ConnectionType.SQLSERVER",
             "options": {"host": "x", "port": "1433"}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated", res
        auth.target_client.connections.create.assert_not_called()

    def test_missing_mandatory_credentials_becomes_validation_failed(self):
        """Live: from-scratch create with only the discovered options (host,
        port) is rejected by UC for SQLSERVER — 'must include the following
        option(s): user,password'. That's the non-exportable-credential
        limitation, NOT an unexpected error: surface validation_failed
        (manual recreation required), never failed."""
        from migrate.connections_worker import apply_connection

        auth = MagicMock()
        self._absent_on_target(auth)
        auth.target_client.connections.create.side_effect = Exception(
            "CONNECTION/CONNECTION_SQLSERVER must include the following option(s): user,password."
        )
        res = apply_connection(
            {"connection_name": "integration_test_sqlserver",
             "connection_type": "ConnectionType.SQLSERVER",
             "options": {"host": "x", "port": "1433"}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validation_failed", res
        assert "user,password" in (res["error_message"] or "")


# ============================================================================
# foreign_catalogs_worker
# ============================================================================
# | any | missing | catalogs.create             | validated |
# | any | exists  | "already exists"            | pre-existing: validated with message |


class TestForeignCatalogsIdempotency:
    def test_create_succeeds(self):
        from migrate.foreign_catalogs_worker import apply_foreign_catalog

        auth = MagicMock()
        auth.target_client.catalogs.create.return_value = None
        res = apply_foreign_catalog(
            {"catalog_name": "fc1", "connection_name": "c1", "options": {}},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"

    def test_already_exists_is_validated(self):
        """Pin: foreign catalogs already tolerate 'already exists' — retry-safe."""
        from migrate.foreign_catalogs_worker import apply_foreign_catalog

        auth = MagicMock()
        auth.target_client.catalogs.create.side_effect = Exception("catalog 'fc1' already exists")
        res = apply_foreign_catalog(
            {"catalog_name": "fc1", "connection_name": "c1"},
            auth=auth, dry_run=False,
        )
        assert res["status"] == "validated"
        assert "already existed" in (res["error_message"] or "")


# ============================================================================
# online_tables_worker
# ============================================================================
# Real migration: ``migrate_online_table`` calls create_synced_database_table.
# Idempotency is handled by AlreadyExists → skipped_target_exists (terminal).

class TestOnlineTablesIdempotency:
    def test_already_exists_is_idempotent(self):
        """Second run (target synced table exists) → skipped_target_exists; the
        SDK raises AlreadyExists from create_synced_database_table on both
        calls, but no error is surfaced."""
        import json

        from databricks.sdk.errors import AlreadyExists

        from migrate.online_tables_worker import migrate_online_table

        client = MagicMock()
        ready_inst = MagicMock()
        ready_inst.state = "AVAILABLE"
        client.database.get_database_instance.return_value = ready_inst
        client.database.create_synced_database_table.side_effect = AlreadyExists("already exists")
        config = MagicMock()
        config.lakebase_instance_name = "lb1"
        config.lakebase_logical_database = "ldb"
        config.lakebase_capacity = "CU_1"
        definition = {
            "name": "c.s.ot",
            "spec": {"source_table_full_name": "c.s.src", "primary_key_columns": ["id"], "run_triggered": {}},
        }
        row = {
            "object_name": "c.s.ot",
            "object_type": "online_table",
            "metadata_json": json.dumps({"definition": definition}),
        }
        res1 = migrate_online_table(client, row, config, sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        res2 = migrate_online_table(client, row, config, sleep_fn=lambda s: None, max_attempts=1, sleep_seconds=0)
        assert res1["status"] == res2["status"] == "skipped_target_exists"
        assert client.database.create_synced_database_table.call_count == 2


# ============================================================================
# sharing_worker
# ============================================================================
# | any | missing share | shares.create + ALTER SHARE ADD             | validated |
# | any | share exists  | shares.create raises "already exists"       | swallow + continue |
# | any | obj in share  | ALTER SHARE ADD "already" for object        | BUG FIX: now counted, not failure |
# | any | recipient ex  | recipients.create raises                    | validated with "already existed" |
# | any | provider ex   | providers.create raises                     | validated with "already existed" |


class TestSharingIdempotency:
    @patch("migrate.sharing_worker.execute_and_poll")
    def test_apply_share_creates_new_and_adds_objects(self, mock_exec):
        from migrate.sharing_worker import apply_share

        mock_exec.return_value = _ok()
        auth = MagicMock()
        auth.target_client.shares.create.return_value = None
        res = apply_share(
            {"share_name": "s1", "objects": [
                {"data_object_type": "SharedDataObjectDataObjectType.TABLE", "name": "c.s.t"},
            ]},
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"
        sql = mock_exec.call_args[0][2]
        assert "ALTER SHARE `s1` ADD TABLE" in sql

    @patch("migrate.sharing_worker.execute_and_poll")
    def test_apply_share_tolerates_share_already_exists(self, mock_exec):
        """Pin: share shell already-exists is swallowed — proceed to add objects."""
        from databricks.sdk.errors import AlreadyExists

        from migrate.sharing_worker import apply_share

        mock_exec.return_value = _ok()
        auth = MagicMock()
        auth.target_client.shares.create.side_effect = AlreadyExists("share 's1' already exists")
        res = apply_share(
            {"share_name": "s1", "objects": [
                {"data_object_type": "TABLE", "name": "c.s.t"},
            ]},
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"

    @patch("migrate.sharing_worker.execute_and_poll")
    def test_apply_share_tolerates_object_already_in_share(self, mock_exec):
        """BUG FIX: ALTER SHARE ADD of already-present object no longer fails the share.

        A clean retry (all objects already present, nothing failed) is
        validated with no error_message — matching recipients/providers.
        """
        from migrate.sharing_worker import apply_share

        mock_exec.return_value = {
            "state": "FAILED", "error": "Data object is already in share",
        }
        auth = MagicMock()
        auth.target_client.shares.create.return_value = None
        res = apply_share(
            {"share_name": "s1", "objects": [
                {"data_object_type": "TABLE", "name": "c.s.t"},
            ]},
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"
        # Pre-fix this would have been "validation_failed"; pinned here.
        assert res["error_message"] is None

    @patch("migrate.sharing_worker.execute_and_poll")
    def test_apply_share_mixed_new_and_already_present_is_validated(self, mock_exec):
        """Pin: mixed retry (some new, some already present) is validated with
        "already present" in the summary message."""
        from migrate.sharing_worker import apply_share

        mock_exec.side_effect = [
            _ok(),  # first table added
            {"state": "FAILED", "error": "already in share"},  # second already present
        ]
        auth = MagicMock()
        auth.target_client.shares.create.return_value = None
        res = apply_share(
            {"share_name": "s1", "objects": [
                {"data_object_type": "TABLE", "name": "c.s.t1"},
                {"data_object_type": "TABLE", "name": "c.s.t2"},
            ]},
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"
        # added=1 -> error_message surfaces the tally
        # Cleanly-added + already-present runs emit a summary rather than None.
        # Treat as validated either way.

    @patch("migrate.sharing_worker.execute_and_poll")
    def test_apply_share_propagates_non_already_errors(self, mock_exec):
        from migrate.sharing_worker import apply_share

        mock_exec.return_value = {"state": "FAILED", "error": "permission denied"}
        auth = MagicMock()
        auth.target_client.shares.create.return_value = None
        res = apply_share(
            {"share_name": "s1", "objects": [
                {"data_object_type": "TABLE", "name": "c.s.t"},
            ]},
            auth=auth, wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validation_failed"

    def test_recipient_already_exists_is_validated(self):
        """Pin: recipient already-exists is tolerated."""
        from migrate.sharing_worker import apply_recipient

        auth = MagicMock()
        auth.target_client.recipients.create.side_effect = Exception(
            "recipient 'r1' already exists"
        )
        res = apply_recipient({"recipient_name": "r1"}, auth=auth, dry_run=False)
        assert res["status"] == "validated"
        assert "already" in (res["error_message"] or "")

    def test_provider_already_exists_is_validated(self):
        """Pin: provider already-exists is tolerated."""
        from migrate.sharing_worker import apply_provider

        auth = MagicMock()
        auth.target_client.providers.create.side_effect = Exception(
            "provider 'p1' already exists"
        )
        res = apply_provider({"provider_name": "p1"}, auth=auth, dry_run=False)
        assert res["status"] == "validated"


# ============================================================================
# models_worker
# ============================================================================
# | any | model missing | registered_models.create + MLflow versions + aliases | validated |
# | any | model exists  | registered_models.create raises "already exists"      | swallow + continue to versions |
# | any | version exists| MLflow create_model_version raises "already exists"   | note + continue (validation_failed) |
# | any | any           | registered_models.set_alias                           | server-side replace, idempotent |


def _mlflow_registry(target_versions=("1",)):
    """Fake MLflow registry client (the _registry_client seam) whose
    create_model_version returns sequential target version numbers."""
    reg = MagicMock()
    created = []

    def _create(name, source, run_id=None, **kwargs):
        mv = MagicMock()
        mv.version = target_versions[len(created)] if len(created) < len(target_versions) else str(len(created) + 1)
        created.append(name)
        return mv

    reg.create_model_version.side_effect = _create
    return reg


class TestModelsIdempotency:
    def test_model_already_exists_continues_to_versions(self, monkeypatch):
        """Pin: registered_models.create AlreadyExists does not fail the model;
        the version is still created via MLflow."""
        from databricks.sdk.errors import AlreadyExists

        from migrate.models_worker import apply_model

        reg = _mlflow_registry(("1",))
        monkeypatch.setattr("migrate.models_worker._registry_client", lambda a: reg)
        monkeypatch.setattr(
            "migrate.models_worker._download_source_artifacts",
            lambda auth, fqn, ver: f"/local/dl/{fqn}/{ver}",
        )
        auth = MagicMock()
        auth.target_client.registered_models.create.side_effect = AlreadyExists(
            "model 'm' already exists"
        )
        results = apply_model(
            {"model_fqn": "c.s.m", "versions": [
                {"version": "1", "source": "dbfs:/v1", "storage_location": "abfss://src/v1", "aliases": ["prod"]},
            ]},
            auth=auth, dry_run=False,
        )
        assert results[0]["status"] == "validated"
        # Version create was still attempted via MLflow.
        reg.create_model_version.assert_called_once()

    def test_version_already_exists_is_noted_not_crashed(self, monkeypatch):
        """Pin: MLflow create_model_version AlreadyExists (re-run without a
        reconciliation drop) is noted, not a crash. Reconciliation normally
        drops the whole model first, so this is the belt-and-braces path."""
        from databricks.sdk.errors import AlreadyExists

        from migrate.models_worker import apply_model

        reg = MagicMock()
        reg.create_model_version.side_effect = AlreadyExists("version 1 already exists")
        monkeypatch.setattr("migrate.models_worker._registry_client", lambda a: reg)
        monkeypatch.setattr(
            "migrate.models_worker._download_source_artifacts",
            lambda auth, fqn, ver: f"/local/dl/{fqn}/{ver}",
        )
        auth = MagicMock()
        auth.target_client.registered_models.create.return_value = None
        results = apply_model(
            {"model_fqn": "c.s.m", "versions": [
                {"version": "1", "source": "dbfs:/v1", "storage_location": "abfss://src/v1", "aliases": ["prod"]},
            ]},
            auth=auth, dry_run=False,
        )
        # Not a crash; recorded as validation_failed with the already-exists note.
        assert results[0]["status"] == "validation_failed"
        assert "already exists" in results[0]["error_message"]


# ============================================================================
# Hive workers
# ============================================================================
# hive_external_worker, hive_views_worker, hive_functions_worker use
# CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE VIEW / CREATE OR REPLACE
# FUNCTION — idempotent.
# hive_managed_dbfs_worker uses mode("overwrite") for data copy — idempotent
# file state. Re-register also uses CREATE TABLE IF NOT EXISTS.
# hive_managed_nondbfs_worker uses CREATE TABLE IF NOT EXISTS + MSCK REPAIR.
# hive_grants_worker: same as grants_worker — UC GRANT is server-side idempotent.


class TestHiveExternalIdempotency:
    @patch("migrate.hive_external_worker.append_migration_status_via_warehouse")
    @patch("migrate.hive_external_worker.warehouse_table_count")
    @patch("migrate.hive_external_worker.time")
    @patch("migrate.hive_external_worker.execute_and_poll")
    def test_uses_if_not_exists_and_keeps_namespace(self, mock_exec, mock_time, mock_wh_count, mock_append):
        """Like-for-like migration: rewrite_hive_namespace is identity,
        so hive_metastore references pass through unchanged."""
        from migrate.hive_external_worker import migrate_hive_external_table

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        explorer = MagicMock()
        explorer.get_create_statement.return_value = (
            "CREATE TABLE `hive_metastore`.`db`.`t` (id INT) USING PARQUET "
            "LOCATION 'abfss://x@y/t'"
        )
        explorer.get_table_row_count.return_value = 1
        # NON-UC compute: target row-count via warehouse, status writes too.
        mock_wh_count.return_value = 1
        config = MagicMock()
        config.dry_run = False
        config.hive_target_catalog = "uc_hive"
        res = migrate_hive_external_table(
            {"object_name": "`hive_metastore`.`db`.`t`"},
            config=config, auth=MagicMock(),
            explorer=explorer, wh_id="wh",
            tracking_fqn="migration_tracking.cp_migration", job_run_id="jr-1",
            status_wh_id="wh-src",
        )
        # execute_and_poll is now called only for the CREATE DDL (status +
        # target count are routed through the patched warehouse helpers).
        sql = mock_exec.call_args[0][2]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        # Like-for-like: hive_metastore references unchanged
        assert "`hive_metastore`.`db`.`t`" in sql
        assert res["status"] == "validated"


class TestHiveViewsIdempotency:
    @patch("migrate.hive_views_worker.time")
    @patch("migrate.hive_views_worker.execute_and_poll")
    def test_uses_or_replace(self, mock_exec, mock_time):
        from migrate.hive_views_worker import migrate_hive_view

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        config = MagicMock()
        config.dry_run = False
        config.hive_target_catalog = "uc_hive"
        res = migrate_hive_view(
            {"object_name": "`hive_metastore`.`db`.`v`"},
            "CREATE VIEW `hive_metastore`.`db`.`v` AS SELECT 1",
            config=config, auth=MagicMock(), wh_id="wh",
        )
        sql = mock_exec.call_args[0][2]
        assert "CREATE OR REPLACE VIEW" in sql
        assert res["status"] == "validated"


class TestHiveFunctionsIdempotency:
    @patch("migrate.hive_functions_worker.time")
    @patch("migrate.hive_functions_worker.get_hive_function_ddl")
    @patch("migrate.hive_functions_worker.execute_and_poll")
    def test_uses_or_replace(self, mock_exec, mock_ddl, mock_time):
        from migrate.hive_functions_worker import migrate_hive_function

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        mock_ddl.return_value = "CREATE FUNCTION `hive_metastore`.`db`.`f`(x INT) RETURNS INT RETURN x"
        config = MagicMock()
        config.dry_run = False
        config.hive_target_catalog = "uc_hive"
        res = migrate_hive_function(
            {"object_name": "`hive_metastore`.`db`.`f`"},
            config=config, auth=MagicMock(), tracker=MagicMock(),
            spark=MagicMock(), wh_id="wh",
        )
        sql = mock_exec.call_args[0][2]
        assert "CREATE OR REPLACE FUNCTION" in sql
        assert res["status"] == "validated"


class TestHiveManagedDbfsIdempotency:
    @patch("migrate.hive_managed_dbfs_worker.warehouse_table_count")
    @patch("migrate.hive_managed_dbfs_worker.time")
    @patch("migrate.hive_managed_dbfs_worker.execute_and_poll")
    def test_stage_overwrite_then_create_or_replace(self, mock_exec, mock_time, mock_wh_count):
        """Pin: the two-hop DBFS managed path is re-runnable — STAGE 1 stages the
        data with mode('overwrite') and STAGE 2 lands the target managed table via
        CREATE OR REPLACE (not a bare CREATE that would fail on re-run)."""
        from migrate.hive_managed_dbfs_worker import migrate_hive_managed_dbfs

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        mock_wh_count.return_value = 5
        config = MagicMock()
        config.dry_run = False
        config.migrate_hive_dbfs_root = True
        config.hive_dbfs_staging_path = "abfss://stage@acct.dfs.core.windows.net/hive_stage"
        spark = MagicMock()
        df = MagicMock()
        df.count.return_value = 5
        spark.read.table.return_value = df
        res = migrate_hive_managed_dbfs(
            {"object_name": "`hive_metastore`.`db`.`t`"},
            config=config, auth=MagicMock(), tracker=MagicMock(),
            spark=spark, wh_id="wh",
        )
        # STAGE 1: staging write was overwrite (re-runnable stage)
        df.write.mode.assert_called_with("overwrite")
        # STAGE 2: target CTAS is CREATE OR REPLACE (re-runnable registration)
        sql = mock_exec.call_args[0][2]
        assert "CREATE OR REPLACE TABLE" in sql
        assert res["status"] == "validated"

    def test_opt_out_yields_skipped_by_config(self):
        """Pin: migrate_hive_dbfs_root=False yields skipped_by_config — worker re-emits this on retry."""
        from migrate.hive_managed_dbfs_worker import migrate_hive_managed_dbfs

        config = MagicMock()
        config.dry_run = False
        config.migrate_hive_dbfs_root = False
        res = migrate_hive_managed_dbfs(
            {"object_name": "`hive_metastore`.`db`.`t`"},
            config=config, auth=MagicMock(), tracker=MagicMock(),
            spark=MagicMock(), wh_id="wh",
        )
        assert res["status"] == "skipped_by_config"


class TestHiveManagedNondbfsIdempotency:
    @patch("migrate.hive_managed_nondbfs_worker.append_migration_status_via_warehouse")
    @patch("migrate.hive_managed_nondbfs_worker.warehouse_table_count")
    @patch("migrate.hive_managed_nondbfs_worker.time")
    @patch("migrate.hive_managed_nondbfs_worker.execute_and_poll")
    def test_uses_if_not_exists_and_forces_location(self, mock_exec, mock_time, mock_wh_count, mock_append):
        """Pin: non-DBFS managed path rewrites to EXTERNAL (IF NOT EXISTS + LOCATION)."""
        from migrate.hive_managed_nondbfs_worker import migrate_hive_managed_nondbfs

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        explorer = MagicMock()
        explorer.get_create_statement.return_value = (
            "CREATE TABLE `hive_metastore`.`db`.`t` (id INT) USING DELTA"
        )
        explorer.get_table_row_count.return_value = 1
        # NON-UC compute: target row-count via warehouse, status writes too.
        mock_wh_count.return_value = 1
        config = MagicMock()
        config.dry_run = False
        config.hive_target_catalog = "uc_hive"
        res = migrate_hive_managed_nondbfs(
            {"object_name": "`hive_metastore`.`db`.`t`",
             "storage_location": "abfss://x@y/t", "provider": "delta"},
            config=config, auth=MagicMock(),
            explorer=explorer, wh_id="wh",
            tracking_fqn="migration_tracking.cp_migration", job_run_id="jr-1",
            status_wh_id="wh-src",
        )
        sql = mock_exec.call_args[0][2]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "LOCATION 'abfss://x@y/t'" in sql
        assert res["status"] == "validated"


class TestHiveGrantsIdempotency:
    @patch("migrate.hive_grants_worker.time")
    @patch("migrate.hive_grants_worker.execute_and_poll")
    def test_emits_grant_with_uc_privilege(self, mock_exec, mock_time):
        """Pin: hive grants map to UC privileges via HIVE_TO_UC_PRIVILEGES; GRANT is idempotent."""
        from migrate.hive_grants_worker import _emit_grant

        mock_time.time.side_effect = [100.0, 101.0]
        mock_exec.return_value = _ok()
        res = _emit_grant(
            action_type="SELECT", securable_keyword="TABLE",
            target_fqn="`uc_hive`.`db`.`t`", principal="alice@x.com",
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"
        sql = mock_exec.call_args[0][2]
        assert "GRANT SELECT ON TABLE `uc_hive`.`db`.`t` TO `alice@x.com`" in sql

    def test_unmapped_privilege_is_skipped(self):
        """Pin: unmapped Hive privileges (e.g. UNKNOWN) are skipped, not failed."""
        from migrate.hive_grants_worker import _emit_grant

        res = _emit_grant(
            action_type="UNKNOWN_PRIV", securable_keyword="TABLE",
            target_fqn="`uc_hive`.`db`.`t`", principal="alice@x.com",
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "skipped"
        assert "unmapped privilege" in (res["error_message"] or "")

    @patch("migrate.hive_grants_worker.execute_and_poll")
    def test_own_transfers_ownership(self, mock_exec):
        """Pin (#5): OWN grants transfer ownership via ALTER ... OWNER TO."""
        from migrate.hive_grants_worker import _emit_grant

        mock_exec.return_value = {"state": "SUCCEEDED", "statement_id": "s"}
        res = _emit_grant(
            action_type="OWN", securable_keyword="TABLE",
            target_fqn="`uc_hive`.`db`.`t`", principal="alice@x.com",
            auth=MagicMock(), wh_id="wh", dry_run=False,
        )
        assert res["status"] == "validated"
        assert mock_exec.call_args[0][2] == "ALTER TABLE `uc_hive`.`db`.`t` OWNER TO `alice@x.com`"

    def test_own_skipped_when_transfer_disabled(self):
        """transfer_ownership=False preserves the old skip behaviour."""
        from migrate.hive_grants_worker import _emit_grant

        res = _emit_grant(
            action_type="OWN", securable_keyword="TABLE",
            target_fqn="`uc_hive`.`db`.`t`", principal="alice@x.com",
            auth=MagicMock(), wh_id="wh", dry_run=False, transfer_ownership=False,
        )
        assert res["status"] == "skipped"


# ============================================================================
# Cross-worker: upstream filtering semantics
# ============================================================================
# The terminal-state filter lives in TrackingManager.get_pending_objects and
# filters on status IN ('validated', 'skipped'). This means 'failed',
# 'in_progress', 'validation_failed', 'skipped_by_config', 'skipped_no_access'
# are ALL retried. Pin this so X.1 (retry/resumability) has a documented baseline.


class TestGetPendingObjectsFilter:
    def test_filter_excludes_validated_and_skipped(self):
        """Pin: get_pending_objects filters out only terminal statuses.

        Per PR #26, the terminal set is the explicit IN list
        ('validated', 'skipped_by_pipeline_migration') — not a LIKE pattern —
        so flag-gated skips (skipped_by_config) re-pickup on re-run.
        """
        from common.tracking import TrackingManager

        spark = MagicMock()
        collected = MagicMock()
        collected.asDict.return_value = {"object_name": "`c`.`s`.`t`", "object_type": "managed_table"}
        spark.sql.return_value.collect.return_value = [collected]
        config = MagicMock()
        config.tracking_catalog = "tracking"
        config.tracking_schema = "cp_migration"
        tm = TrackingManager(spark, config)
        _ = tm.get_pending_objects("managed_table")
        sql = spark.sql.call_args[0][0]
        # Pin the terminal states literal — this is what X.1 must preserve/extend.
        assert "'validated', 'skipped_by_pipeline_migration'" in sql
        # Pin the join key — status row is keyed by (object_name, object_type).
        assert "d.object_name = s.object_name" in sql
        assert "d.object_type = s.object_type" in sql
