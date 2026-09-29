from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied

from common.config import MigrationConfig
from pre_check.pre_check import run


def _make_config(**overrides) -> MigrationConfig:
    defaults = dict(
        source_workspace_url="https://source.test",
        target_workspace_url="https://target.test",
        spn_client_id="test-id",
        spn_secret_scope="scope",
        spn_secret_key="key",
    )
    defaults.update(overrides)
    return MigrationConfig(**defaults)


class TestPreCheck:
    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_all_checks_pass(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()

        mock_auth = mock_auth_cls.return_value
        mock_auth.test_connectivity.return_value = {"source": True, "target": True}
        # New SDK exposes list_shares() (not list()); the pre_check shim (#5)
        # must use it so the sharing preflight PASSes instead of WARNing.
        mock_auth.source_client.shares = MagicMock(spec=["list_shares"])
        mock_auth.source_client.shares.list_shares.return_value = []
        mock_auth.target_client.shares = MagicMock(spec=["list_shares"])
        mock_auth.target_client.shares.list_shares.return_value = []
        mock_auth.target_client.metastores.summary.return_value = MagicMock(name="test-metastore")
        mock_auth.target_client.storage_credentials.list.return_value = [MagicMock()]
        mock_auth.target_client.external_locations.list.return_value = [MagicMock()]

        mock_explorer_cls.return_value.list_catalogs.return_value = ["cat_a"]
        spark.sql.return_value.first.return_value = MagicMock(ms="test-metastore-id")
        # check_deploy_is_source: deploy workspace == configured source URL
        spark.conf.get.return_value = "https://source.test"

        # Phase 2 additions need a source_client that supports cluster/warehouse listing
        mock_auth.source_client.clusters.list.return_value = []
        mock_auth.source_client.warehouses.list.return_value = []
        # Hive enumeration used by check_hive_dbfs_root_config — succeed with no DBFs
        mock_explorer_cls.return_value.list_hive_databases.return_value = []
        mock_explorer_cls.return_value.classify_hive_tables.return_value = []

        # spark.sql default MagicMock returns an empty collect() which
        # the collision-detection check reads as "discovery_inventory
        # empty" → PASS.

        results = run(dbutils, spark)

        # 10 core UC checks + 3 Hive/external-metastore checks +
        # 1 target-collision check (X.4) + 1 deploy-is-source guard (#3) +
        # 3 like-for-like Hive guards (target DBFS-root, /mnt mounts,
        # staging-reachable) + 1 target-warehouse DAC guard (#25) = 19
        assert len(results) == 19
        assert all(r["status"] in ("PASS", "WARN") for r in results)
        assert sum(1 for r in results if r["status"] == "FAIL") == 0
        _deploy = [r for r in results if r["check_name"] == "check_deploy_is_source"]
        assert _deploy and _deploy[0]["status"] == "PASS"
        # #5: sharing preflight must PASS via the list_shares shim (not WARN)
        _sharing = {r["check_name"]: r["status"] for r in results
                    if r["check_name"] in ("check_source_sharing", "check_target_sharing")}
        assert _sharing == {"check_source_sharing": "PASS", "check_target_sharing": "PASS"}

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_deploy_to_target_fails(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        # Regression guard for finding #3: if the bundle is deployed to the
        # TARGET workspace, check_deploy_is_source must FAIL (not silently
        # let discovery scan the empty target as the source).
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        mock_auth = mock_auth_cls.return_value
        mock_auth.test_connectivity.return_value = {"source": True, "target": True}
        mock_auth.source_client.shares = MagicMock(spec=["list_shares"])
        mock_auth.source_client.shares.list_shares.return_value = []
        mock_auth.target_client.shares = MagicMock(spec=["list_shares"])
        mock_auth.target_client.shares.list_shares.return_value = []
        mock_auth.target_client.metastores.summary.return_value = MagicMock(name="test-metastore")
        mock_auth.target_client.storage_credentials.list.return_value = [MagicMock()]
        mock_auth.target_client.external_locations.list.return_value = [MagicMock()]
        mock_explorer_cls.return_value.list_catalogs.return_value = ["cat_a"]
        spark.sql.return_value.first.return_value = MagicMock(ms="test-metastore-id")
        mock_auth.source_client.clusters.list.return_value = []
        mock_auth.source_client.warehouses.list.return_value = []
        mock_explorer_cls.return_value.list_hive_databases.return_value = []
        mock_explorer_cls.return_value.classify_hive_tables.return_value = []
        # Deploy workspace URL == configured TARGET (the misconfiguration)
        spark.conf.get.return_value = "https://target.test"

        # Every other check passes in this setup, so a raise here proves the
        # deploy-is-source guard is the sole FAIL ("1 check(s) returned FAIL").
        with pytest.raises(Exception, match=r"1 check\(s\) returned FAIL"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_auth_failure_raises(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()

        mock_auth = mock_auth_cls.return_value
        mock_auth.test_connectivity.return_value = {"source": False, "target": True}
        mock_auth.target_client.metastores.summary.return_value = MagicMock(name="ms")
        mock_auth.source_client.shares.list.return_value = []
        mock_auth.target_client.shares.list.return_value = []
        mock_auth.target_client.storage_credentials.list.return_value = []
        mock_auth.target_client.external_locations.list.return_value = []
        mock_explorer_cls.return_value.list_catalogs.return_value = []

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)


def _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark):
    """Wire up the default 'all green' mocks. Individual tests override one
    piece to assert that specific check FAILs cleanly."""
    mock_auth = mock_auth_cls.return_value
    mock_auth.test_connectivity.return_value = {"source": True, "target": True}
    mock_auth.source_client.shares.list.return_value = []
    mock_auth.target_client.shares.list.return_value = []
    mock_auth.target_client.metastores.summary.return_value = MagicMock(name="ms")
    mock_auth.target_client.storage_credentials.list.return_value = [MagicMock()]
    mock_auth.target_client.external_locations.list.return_value = [MagicMock()]
    mock_auth.source_client.clusters.list.return_value = []
    mock_auth.source_client.warehouses.list.return_value = []

    mock_explorer_cls.return_value.list_catalogs.return_value = ["cat_a"]
    mock_explorer_cls.return_value.list_hive_databases.return_value = []
    mock_explorer_cls.return_value.classify_hive_tables.return_value = []

    spark.sql.return_value.first.return_value = MagicMock(ms="metastore-id")
    return mock_auth


class TestPreCheckIndividualFailures:
    """Per-check failure tests — each one isolates one precondition so the
    FAIL path for that check is specifically exercised. Complements the
    happy path + generic-auth-failure tests above."""

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_target_auth_failure(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        """If target connectivity fails, ``check_target_auth`` FAILs and
        ``run()`` raises (because at least one check failed)."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        mock_auth = _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)
        mock_auth.test_connectivity.return_value = {"source": True, "target": False}

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_source_metastore_lookup_failure(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        """spark.sql(SELECT current_metastore()) raising bubbles into
        check_source_metastore FAILing with the action-required hint."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)

        # Make only the metastore query fail.
        def _sql_side_effect(query, *args, **kwargs):
            r = MagicMock()
            if "current_metastore()" in query:
                raise RuntimeError("metastore unreachable")
            r.first.return_value = MagicMock()
            return r

        spark.sql.side_effect = _sql_side_effect

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_target_metastore_lookup_failure(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        """auth.target_client.metastores.summary raising is caught by
        check_target_metastore's try/except."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        mock_auth = _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)
        mock_auth.target_client.metastores.summary.side_effect = RuntimeError("target metastore lookup failed")

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_source_catalog_enumeration_failure(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """list_catalogs raising → check_source_catalogs FAILs."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)
        mock_explorer_cls.return_value.list_catalogs.side_effect = RuntimeError("permission denied on metastore")

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_all_checks_recorded_even_when_one_fails(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """One check failing must not abort the rest — run() should
        continue through all checks and record each so the operator sees
        the full picture in one report."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        mock_auth = _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)
        mock_auth.target_client.metastores.summary.side_effect = RuntimeError("oops")

        # Check the tracker.append_pre_check_results got ALL checks,
        # not just the first few before the failure.
        import contextlib

        tracker = mock_tracker_cls.return_value
        # raise is expected once at least one check failed
        with contextlib.suppress(Exception):
            run(dbutils, spark)
        # At least one call with >=5 entries (confirms we ran the full
        # battery; exact count varies with enabled checks).
        recorded = tracker.append_pre_check_results.call_args_list
        assert recorded, "No pre-check results were written."
        total_rows = sum(len(c.args[0]) for c in recorded)
        assert total_rows >= 5, (
            f"Only {total_rows} pre-check result(s) recorded — one check failing should not abort the rest."
        )



def _setup_collision_mocks(
    mock_auth_cls,
    mock_tracker_cls,
    mock_explorer_cls,
    spark,
    discovery_rows=None,
    status_rows=None,
    target_hits=None,
):
    """Wire the collision-detection dependencies: discovery_inventory SQL
    returns the given rows, migration_status returns the given status
    rows, and target_client.tables.get raises or returns based on
    target_hits (a set of target FQNs present on target)."""
    mock_auth = _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)

    discovery_rows = discovery_rows or []
    status_rows = status_rows or []
    target_hits = target_hits or set()

    # Build spark.sql side_effect: depending on query, return the right
    # MagicMock.collect() output. The collision check issues TWO queries:
    # one for discovery_inventory, one for migration_status.
    def _sql_side_effect(query, *args, **kwargs):
        r = MagicMock()
        q = query if isinstance(query, str) else str(query)
        if "discovery_inventory" in q and "SELECT object_name" in q:
            mocks = []
            for row in discovery_rows:
                m = MagicMock()
                m.object_name = row["object_name"]
                m.object_type = row["object_type"]
                m.source_type = row.get("source_type", "uc")
                mocks.append(m)
            r.collect.return_value = mocks
            return r
        if "migration_status" in q and "SELECT DISTINCT" in q:
            mocks = []
            for srow in status_rows:
                m = MagicMock()
                m.object_name = srow["object_name"]
                m.object_type = srow["object_type"]
                mocks.append(m)
            r.collect.return_value = mocks
            return r
        if "current_metastore()" in q:
            r.first.return_value = MagicMock(ms="metastore-id")
            return r
        # Default
        r.first.return_value = MagicMock()
        return r

    spark.sql.side_effect = _sql_side_effect

    # Wire target_client probes based on target_hits
    def _tables_get(full_name):
        if full_name in target_hits:
            return MagicMock()
        raise NotFound("NOT_FOUND")

    def _catalogs_get(name):
        if name in target_hits:
            return MagicMock()
        raise NotFound("NOT_FOUND")

    mock_auth.target_client.tables.get.side_effect = _tables_get
    mock_auth.target_client.catalogs.get.side_effect = _catalogs_get
    mock_auth.target_client.schemas.get.side_effect = lambda full_name: (
        MagicMock() if full_name in target_hits else (_ for _ in ()).throw(NotFound("NOT_FOUND"))
    )
    mock_auth.target_client.functions.get.side_effect = lambda name: (
        MagicMock() if name in target_hits else (_ for _ in ()).throw(NotFound("NOT_FOUND"))
    )
    mock_auth.target_client.volumes.read.side_effect = lambda name: (
        MagicMock() if name in target_hits else (_ for _ in ()).throw(NotFound("NOT_FOUND"))
    )
    return mock_auth


class TestPreCheckCollisionDetection:
    """X.4: pre_check's new check_target_collisions row.

    Exercises each branch:
    - discovery_inventory empty → PASS, no WARN
    - no target hits → PASS
    - target hits under default on_target_collision=fail → FAIL
    - target hits under on_target_collision=skip → WARN + seeded
      skipped_target_exists migration_status rows
    - X.2 compatibility: status-row-present objects are NOT re-probed
      (no duplicate collision row)
    """

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_empty_discovery_is_pass(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        _setup_collision_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark, discovery_rows=[])

        results = run(dbutils, spark)
        coll = [r for r in results if r["check_name"] == "check_target_collisions"]
        assert len(coll) == 1
        assert coll[0]["status"] == "PASS"
        assert "empty" in coll[0]["message"].lower()

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_no_target_hits_is_pass(self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file):
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        _setup_collision_mocks(
            mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark,
            discovery_rows=[
                {"object_name": "`c`.`s`.`t`", "object_type": "managed_table", "source_type": "uc"}
            ],
            target_hits=set(),
        )

        results = run(dbutils, spark)
        coll = [r for r in results if r["check_name"] == "check_target_collisions"]
        assert len(coll) == 1
        assert coll[0]["status"] == "PASS"

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_collision_under_fail_policy_emits_fail(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """Default on_target_collision=fail: collision rows get status=FAIL
        and pre_check itself raises (because at least one check failed)."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(on_target_collision="fail")
        _setup_collision_mocks(
            mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark,
            discovery_rows=[
                {"object_name": "`c`.`s`.`t`", "object_type": "managed_table", "source_type": "uc"}
            ],
            target_hits={"c.s.t"},
        )

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_collision_probe_error_fails_closed(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """Review finding #10: if the target probe errors (e.g. the SPN lacks
        read access), collision detection must FAIL closed — not silently pass
        — even under on_target_collision=skip."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(on_target_collision="skip")
        _setup_collision_mocks(
            mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark,
            discovery_rows=[
                {"object_name": "`c`.`s`.`t`", "object_type": "managed_table", "source_type": "uc"}
            ],
            target_hits=set(),
        )
        # Probe errors with a non-NotFound error → must not read as "absent".
        mock_auth_cls.return_value.target_client.tables.get.side_effect = PermissionDenied(
            "SPN lacks read on target"
        )

        with pytest.raises(Exception, match="Pre-check failed"):
            run(dbutils, spark)
        # Even under skip policy, the unverifiable object must NOT be turned
        # into a skipped_target_exists row.
        appended = mock_tracker_cls.return_value.append_migration_status.call_args_list
        seeded = [
            row for call in appended for row in (call.args[0] if call.args else [])
            if isinstance(row, dict) and row.get("status") == "skipped_target_exists"
        ]
        assert seeded == []

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_collision_under_skip_policy_emits_warn_and_seeds_status(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """on_target_collision=skip: collision rows are WARN, and pre_check
        calls tracker.append_migration_status with skipped_target_exists."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(on_target_collision="skip")
        _setup_collision_mocks(
            mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark,
            discovery_rows=[
                {"object_name": "`c`.`s`.`t`", "object_type": "managed_table", "source_type": "uc"},
                {"object_name": "`c`.`s`.`v`", "object_type": "view", "source_type": "uc"},
            ],
            target_hits={"c.s.t", "c.s.v"},
        )

        results = run(dbutils, spark)

        coll = [r for r in results if r["check_name"] == "check_target_collisions"]
        assert len(coll) == 1
        assert coll[0]["status"] == "WARN"
        assert "skipped_target_exists" in coll[0]["message"]

        # Verify tracker.append_migration_status was called with the skip rows
        tracker = mock_tracker_cls.return_value
        tracker.append_migration_status.assert_called_once()
        seeded = tracker.append_migration_status.call_args[0][0]
        assert len(seeded) == 2
        assert all(r["status"] == "skipped_target_exists" for r in seeded)
        assert {r["object_type"] for r in seeded} == {"managed_table", "view"}

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_status_row_blocks_collision(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """X.2 compatibility: on re-run of migrate, discovery has a
        discovery row AND a migration_status row for the same (type, name).
        The status row means the object is ours — collision detection
        must NOT fire, else X.2's idempotency is broken."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(on_target_collision="fail")
        _setup_collision_mocks(
            mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark,
            discovery_rows=[
                {"object_name": "`c`.`s`.`t`", "object_type": "managed_table", "source_type": "uc"}
            ],
            status_rows=[
                {"object_name": "`c`.`s`.`t`", "object_type": "managed_table"}
            ],
            target_hits={"c.s.t"},  # target has it — but we own it
        )

        results = run(dbutils, spark)
        coll = [r for r in results if r["check_name"] == "check_target_collisions"]
        assert len(coll) == 1
        assert coll[0]["status"] == "PASS"

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_collision_check_records_per_type_summary(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """Message should break down the counts per object_type so
        operators can triage quickly."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config(on_target_collision="skip")
        _setup_collision_mocks(
            mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark,
            discovery_rows=[
                {"object_name": "`c`.`s`.`t1`", "object_type": "managed_table", "source_type": "uc"},
                {"object_name": "`c`.`s`.`t2`", "object_type": "managed_table", "source_type": "uc"},
                {"object_name": "`c`.`s`.`v`", "object_type": "view", "source_type": "uc"},
            ],
            target_hits={"c.s.t1", "c.s.t2", "c.s.v"},
        )

        results = run(dbutils, spark)
        coll = [r for r in results if r["check_name"] == "check_target_collisions"]
        assert len(coll) == 1
        assert "managed_table=2" in coll[0]["message"]
        assert "view=1" in coll[0]["message"]

    @patch("pre_check.pre_check.MigrationConfig.from_workspace_file")
    @patch("pre_check.pre_check.AuthManager")
    @patch("pre_check.pre_check.TrackingManager")
    @patch("pre_check.pre_check.CatalogExplorer")
    def test_discovery_query_failure_is_warn_not_fail(
        self, mock_explorer_cls, mock_tracker_cls, mock_auth_cls, mock_from_file
    ):
        """If discovery_inventory doesn't exist (fresh install before
        discovery ran), collision detection emits WARN with a pointer to
        run discovery — not FAIL."""
        dbutils = MagicMock()
        spark = MagicMock()
        mock_from_file.return_value = _make_config()
        _base_mocks(mock_auth_cls, mock_tracker_cls, mock_explorer_cls, spark)

        def _sql(query, *args, **kwargs):
            if "discovery_inventory" in str(query):
                raise RuntimeError("TABLE_NOT_FOUND")
            r = MagicMock()
            r.first.return_value = MagicMock(ms="ms")
            return r

        spark.sql.side_effect = _sql

        results = run(dbutils, spark)
        coll = [r for r in results if r["check_name"] == "check_target_collisions"]
        assert len(coll) == 1
        assert coll[0]["status"] == "WARN"
        assert "run discovery" in coll[0]["action_required"].lower()
