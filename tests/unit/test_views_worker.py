from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestMigrateView:
    """Tests for the views_worker.migrate_view function."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        explorer = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "explorer": explorer,
            "wh_id": "wh-vw-1",
        }

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.rewrite_ddl")
    @patch("migrate.views_worker.execute_and_poll")
    def test_migrate_success(self, mock_execute, mock_rewrite, mock_time):
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_rewrite.return_value = "CREATE OR REPLACE VIEW `cat`.`sch`.`v1` AS SELECT * FROM `cat`.`sch`.`tbl`"
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-1"}

        deps = self._make_deps()
        deps[
            "explorer"
        ].get_create_statement.return_value = "CREATE VIEW `cat`.`sch`.`v1` AS SELECT * FROM `cat`.`sch`.`tbl`"

        view_info = {"object_name": "`cat`.`sch`.`v1`"}
        result = migrate_view(view_info, **deps)

        assert result["status"] == "validated"
        assert result["object_type"] == "view"
        assert result["error_message"] is None
        deps["tracker"].append_migration_status.assert_called_once()
        mock_execute.assert_called_once()

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.rewrite_ddl")
    @patch("migrate.views_worker.execute_and_poll")
    def test_migrate_dry_run(self, mock_execute, mock_rewrite, mock_time):
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 100.1]
        mock_rewrite.return_value = "CREATE OR REPLACE VIEW `cat`.`sch`.`v2` AS SELECT 1"

        deps = self._make_deps(dry_run=True)
        deps["explorer"].get_create_statement.return_value = "CREATE VIEW `cat`.`sch`.`v2` AS SELECT 1"

        view_info = {"object_name": "`cat`.`sch`.`v2`"}
        result = migrate_view(view_info, **deps)

        assert result["status"] == "skipped"
        assert result["error_message"] == "dry_run"
        mock_execute.assert_not_called()

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.rewrite_ddl")
    @patch("migrate.views_worker.execute_and_poll")
    def test_migrate_ddl_rewrite(self, mock_execute, mock_rewrite, mock_time):
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        rewritten = "CREATE OR REPLACE VIEW `cat`.`sch`.`v3` AS SELECT id FROM `cat`.`sch`.`tbl`"
        mock_rewrite.return_value = rewritten
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-3"}

        deps = self._make_deps()
        original_ddl = "CREATE VIEW `cat`.`sch`.`v3` AS SELECT id FROM `cat`.`sch`.`tbl`"
        deps["explorer"].get_create_statement.return_value = original_ddl

        view_info = {"object_name": "`cat`.`sch`.`v3`"}
        migrate_view(view_info, **deps)

        # Verify rewrite_ddl was called with the CREATE VIEW pattern
        mock_rewrite.assert_called_once_with(
            original_ddl,
            r"CREATE\s+VIEW\b",
            "CREATE OR REPLACE VIEW",
        )
        # Verify the rewritten DDL (containing OR REPLACE) was passed to execute_and_poll
        mock_execute.assert_called_once_with(deps["auth"], "wh-vw-1", rewritten)
        assert "CREATE OR REPLACE VIEW" in rewritten


class TestViewsWorkerComplexDdls:
    """Views referencing multiple tables, CTEs, or other views get replayed
    verbatim (modulo CREATE → CREATE OR REPLACE). The worker shouldn't
    rewrite the body — only the CREATE keyword. If it ever starts mangling
    query bodies, these tests catch it."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        auth = MagicMock()
        tracker = MagicMock()
        explorer = MagicMock()
        return {
            "config": config,
            "auth": auth,
            "tracker": tracker,
            "explorer": explorer,
            "wh_id": "wh-complex",
        }

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.execute_and_poll")
    def test_multi_table_join_view(self, mock_execute, mock_time):
        """View body joining two tables — rewrite should not touch the
        JOIN, just the CREATE keyword."""
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-j"}

        deps = self._make_deps()
        original = (
            "CREATE VIEW `cat`.`sch`.`orders_with_customers` AS "
            "SELECT o.order_id, o.amount, c.name "
            "FROM `cat`.`sch`.`orders` o "
            "JOIN `cat`.`sch`.`customers` c ON o.customer_id = c.id"
        )
        deps["explorer"].get_create_statement.return_value = original

        migrate_view({"object_name": "`cat`.`sch`.`orders_with_customers`"}, **deps)

        replayed = mock_execute.call_args[0][2]
        assert replayed.startswith("CREATE OR REPLACE VIEW"), (
            f"View DDL not replaced with CREATE OR REPLACE: {replayed[:120]}"
        )
        # Body must be preserved verbatim — join / aliases / etc.
        assert "JOIN `cat`.`sch`.`customers` c ON o.customer_id = c.id" in replayed
        assert "o.order_id, o.amount, c.name" in replayed

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.execute_and_poll")
    def test_cte_view(self, mock_execute, mock_time):
        """View body with a WITH clause (CTE) — preserved verbatim."""
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-cte"}

        deps = self._make_deps()
        original = (
            "CREATE VIEW `cat`.`sch`.`top_customers` AS "
            "WITH top AS (SELECT customer_id, SUM(amount) total "
            "FROM `cat`.`sch`.`orders` GROUP BY customer_id "
            "ORDER BY total DESC LIMIT 10) "
            "SELECT * FROM top"
        )
        deps["explorer"].get_create_statement.return_value = original

        migrate_view({"object_name": "`cat`.`sch`.`top_customers`"}, **deps)
        replayed = mock_execute.call_args[0][2]
        assert replayed.startswith("CREATE OR REPLACE VIEW")
        assert "WITH top AS (" in replayed
        assert "ORDER BY total DESC LIMIT 10" in replayed

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.execute_and_poll")
    def test_view_referencing_another_view(self, mock_execute, mock_time):
        """View referencing another view in the same schema — no special
        handling in the worker (topological ordering is the
        orchestrator's job), but the DDL body must be preserved."""
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-nested"}

        deps = self._make_deps()
        original = (
            "CREATE VIEW `cat`.`sch`.`high_value_recent` AS "
            "SELECT * FROM `cat`.`sch`.`high_value_orders` "
            "WHERE order_date > '2024-01-01'"
        )
        deps["explorer"].get_create_statement.return_value = original

        migrate_view({"object_name": "`cat`.`sch`.`high_value_recent`"}, **deps)
        replayed = mock_execute.call_args[0][2]
        assert replayed.startswith("CREATE OR REPLACE VIEW")
        assert "FROM `cat`.`sch`.`high_value_orders`" in replayed

    @patch("migrate.views_worker.time")
    @patch("migrate.views_worker.execute_and_poll")
    def test_view_with_qualifiers(self, mock_execute, mock_time):
        """View with WINDOW / PARTITION BY / ORDER BY — common analytics
        pattern. All preserved."""
        from migrate.views_worker import migrate_view

        mock_time.time.side_effect = [100.0, 105.0, 110.0]
        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-win"}

        deps = self._make_deps()
        original = (
            "CREATE VIEW `cat`.`sch`.`rank_orders` AS "
            "SELECT customer_id, order_id, amount, "
            "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rnk "
            "FROM `cat`.`sch`.`orders`"
        )
        deps["explorer"].get_create_statement.return_value = original

        migrate_view({"object_name": "`cat`.`sch`.`rank_orders`"}, **deps)
        replayed = mock_execute.call_args[0][2]
        assert "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC)" in replayed


class TestViewDependencySkip:
    """Finding #19: the UC views worker must cascade-skip a view that
    references an object that was not migrated (a failed/skipped base
    table), recording ``skipped_dependency_not_migrated`` instead of
    hard-failing with TABLE_OR_VIEW_NOT_FOUND. Mirrors the proven Hive
    behavior (finding #9) but with UC FQNs (real catalog names)."""

    def test_backticked_dependency_matches(self):
        from migrate.views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `cat`.`sch`.`v` AS SELECT * FROM `cat`.`sch`.`customers`"
        not_migrated = {"`cat`.`sch`.`customers`"}
        assert view_dependency_skip("`cat`.`sch`.`v`", ddl, not_migrated) == "`cat`.`sch`.`customers`"

    def test_dotted_dependency_matches(self):
        from migrate.views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW cat.sch.v AS SELECT * FROM cat.sch.customers"
        not_migrated = {"`cat`.`sch`.`customers`"}
        assert view_dependency_skip("`cat`.`sch`.`v`", ddl, not_migrated) == "`cat`.`sch`.`customers`"

    def test_no_dependency_returns_none(self):
        from migrate.views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `cat`.`sch`.`v` AS SELECT * FROM `cat`.`sch`.`good`"
        assert view_dependency_skip("`cat`.`sch`.`v`", ddl, {"`cat`.`sch`.`customers`"}) is None

    def test_empty_not_migrated_never_skips(self):
        from migrate.views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `cat`.`sch`.`v` AS SELECT * FROM `cat`.`sch`.`customers`"
        assert view_dependency_skip("`cat`.`sch`.`v`", ddl, set()) is None

    def test_view_own_fqn_excluded(self):
        """A view whose own FQN is in not_migrated (re-run scenario) must
        NOT self-skip — its own FQN appears in the CREATE header."""
        from migrate.views_worker import view_dependency_skip

        view_fqn = "`cat`.`sch`.`v_orders`"
        ddl = "CREATE OR REPLACE VIEW `cat`.`sch`.`v_orders` AS SELECT * FROM `cat`.`sch`.`good`"
        assert view_dependency_skip(view_fqn, ddl, {view_fqn}) is None

    def test_dotted_prefix_boundary(self):
        """not_migrated contains `orders`; DDL references `orders_2024` —
        must NOT match (token boundary)."""
        from migrate.views_worker import view_dependency_skip

        ddl = "CREATE OR REPLACE VIEW `cat`.`sch`.`v` AS SELECT * FROM cat.sch.orders_2024"
        assert view_dependency_skip("`cat`.`sch`.`v`", ddl, {"`cat`.`sch`.`orders`"}) is None


class TestMigrateViewDependencyCascade:
    """migrate_view records skipped_dependency_not_migrated (and does not
    execute the DDL) when the view references a not-migrated object."""

    def _make_deps(self, *, dry_run: bool = False) -> dict:
        config = MagicMock()
        config.dry_run = dry_run
        return {
            "config": config,
            "auth": MagicMock(),
            "tracker": MagicMock(),
            "explorer": MagicMock(),
            "wh_id": "wh-dep-1",
        }

    @patch("migrate.views_worker.execute_and_poll")
    def test_view_on_not_migrated_table_is_skipped(self, mock_execute):
        from migrate.views_worker import migrate_view

        deps = self._make_deps()
        deps[
            "explorer"
        ].get_create_statement.return_value = (
            "CREATE VIEW `cat`.`sch`.`v_orders` AS SELECT * FROM `cat`.`sch`.`orders`"
        )

        result = migrate_view(
            {"object_name": "`cat`.`sch`.`v_orders`"},
            not_migrated_names={"`cat`.`sch`.`orders`"},
            **deps,
        )

        assert result["status"] == "skipped_dependency_not_migrated"
        assert "`cat`.`sch`.`orders`" in result["error_message"]
        mock_execute.assert_not_called()

    @patch("migrate.views_worker.execute_and_poll")
    def test_view_on_migrated_table_proceeds(self, mock_execute):
        from migrate.views_worker import migrate_view

        mock_execute.return_value = {"state": "SUCCEEDED", "statement_id": "s-ok"}
        deps = self._make_deps()
        deps[
            "explorer"
        ].get_create_statement.return_value = (
            "CREATE VIEW `cat`.`sch`.`v_good` AS SELECT * FROM `cat`.`sch`.`orders`"
        )

        result = migrate_view(
            {"object_name": "`cat`.`sch`.`v_good`"},
            not_migrated_names=set(),
            **deps,
        )

        assert result["status"] == "validated"
        mock_execute.assert_called_once()


class TestViewsWorkerRetryLoop:
    """Covers the retry-on-failure loop in ``views_worker.run()`` added
    to compensate for imperfect topological sort (view_table_usage
    doesn't exist in UC). A view that fails because its upstream wasn't
    created yet should succeed on a later pass.
    """

    def test_retry_picks_up_view_when_upstream_lands_on_later_pass(self):
        """v_b fails pass 1 (upstream v_a not yet there), succeeds pass 2."""
        from migrate import views_worker

        # attempts for v_a and v_b, in order
        # pass 1: v_a ok, v_b fails. pass 2: v_b ok.
        call_log: list[str] = []

        def fake_migrate_view(view_info, **_kwargs):
            fqn = view_info["object_name"]
            call_log.append(fqn)
            if fqn == "`c`.`s`.`v_b`" and call_log.count(fqn) == 1:
                return {
                    "object_name": fqn,
                    "object_type": "view",
                    "status": "failed",
                    "error_message": "TABLE_OR_VIEW_NOT_FOUND v_a",
                    "duration_seconds": 0.1,
                }
            return {
                "object_name": fqn,
                "object_type": "view",
                "status": "validated",
                "error_message": None,
                "duration_seconds": 0.1,
            }

        import json

        dbutils = MagicMock()
        dbutils.jobs.taskValues.get.return_value = json.dumps(
            [
                {"object_name": "`c`.`s`.`v_a`"},
                {"object_name": "`c`.`s`.`v_b`"},
            ]
        )
        spark = MagicMock()

        cfg = MagicMock()
        cfg.dry_run = False

        with (
            patch.object(views_worker.MigrationConfig, "from_workspace_file", return_value=cfg),
            patch.object(views_worker, "AuthManager"),
            patch.object(views_worker, "TrackingManager") as mock_tm,
            patch.object(views_worker, "find_warehouse", return_value="wh"),
            patch.object(views_worker, "CatalogExplorer") as mock_exp,
            patch.object(views_worker, "migrate_view", side_effect=fake_migrate_view),
        ):
            mock_exp.return_value.resolve_view_dependency_order.side_effect = lambda v: list(v)
            views_worker.run(dbutils, spark)

        # v_b should have been attempted twice — pass 1 fail, pass 2 ok
        assert call_log.count("`c`.`s`.`v_b`") == 2
        # final tracking call should contain validated status for both
        final_results = mock_tm.return_value.append_migration_status.call_args_list[-1][0][0]
        statuses = {r["object_name"]: r["status"] for r in final_results}
        assert statuses["`c`.`s`.`v_a`"] == "validated"
        assert statuses["`c`.`s`.`v_b`"] == "validated"

    def test_retry_stops_when_no_progress(self):
        """v_b keeps failing — loop bails after one no-progress pass so we
        don't spin forever.
        """
        from migrate import views_worker

        call_count = {"v_b": 0}

        def fake_migrate_view(view_info, **_kwargs):
            fqn = view_info["object_name"]
            if fqn == "`c`.`s`.`v_b`":
                call_count["v_b"] += 1
                return {
                    "object_name": fqn,
                    "object_type": "view",
                    "status": "failed",
                    "error_message": "TABLE_OR_VIEW_NOT_FOUND permanent",
                    "duration_seconds": 0.1,
                }
            return {
                "object_name": fqn,
                "object_type": "view",
                "status": "validated",
                "error_message": None,
                "duration_seconds": 0.1,
            }

        import json

        dbutils = MagicMock()
        dbutils.jobs.taskValues.get.return_value = json.dumps(
            [
                {"object_name": "`c`.`s`.`v_a`"},
                {"object_name": "`c`.`s`.`v_b`"},
            ]
        )
        spark = MagicMock()

        cfg = MagicMock()
        cfg.dry_run = False

        with (
            patch.object(views_worker.MigrationConfig, "from_workspace_file", return_value=cfg),
            patch.object(views_worker, "AuthManager"),
            patch.object(views_worker, "TrackingManager"),
            patch.object(views_worker, "find_warehouse", return_value="wh"),
            patch.object(views_worker, "CatalogExplorer") as mock_exp,
            patch.object(views_worker, "migrate_view", side_effect=fake_migrate_view),
        ):
            mock_exp.return_value.resolve_view_dependency_order.side_effect = lambda v: list(v)
            views_worker.run(dbutils, spark)

        # pass 1: both attempted (v_a ok, v_b fail). pass 2: v_b retried and fails.
        # pass 2 had no progress -> stop. So v_b attempted 2 times.
        assert call_count["v_b"] == 2
