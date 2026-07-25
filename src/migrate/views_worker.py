# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

# Bootstrap: put the bundle's `src/` dir on sys.path so `from common...` imports resolve
import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass  # not running under a Databricks notebook (e.g. pytest)

# COMMAND ----------
# Views Worker: migrates views from source to target workspace,
# respecting dependency order via topological sort.

import json
import logging
import re
import time

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.sql_utils import execute_and_poll, find_warehouse, rewrite_ddl
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("views_worker")

# Number of passes views_worker will retry still-failing views. Covers
# the case where resolve_view_dependency_order misses an edge (e.g. a
# view that references a table via dynamic SQL that regex can't parse).
_MAX_RETRY_PASSES = 3


# COMMAND ----------


def _is_notebook() -> bool:
    """Return True when running inside a Databricks notebook."""
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


# COMMAND ----------
# Dependency-skip helper (finding #19 — UC counterpart of Hive finding #9)


def view_dependency_skip(view_fqn: str, ddl: str, not_migrated_names: set[str]) -> str | None:
    """Return the FQN of a not-migrated object the view DDL references, else None.

    Takes the view's own FQN to exclude it from consideration (a view's own
    FQN appears in the ``CREATE OR REPLACE VIEW <fqn> AS …`` header and must
    not self-match on re-runs when the view has a non-validated status).

    Matching rules (identical to the Hive worker):
    - Backticked form (e.g. `` `cat`.`sch`.`t` ``): plain substring search —
      backtick boundaries prevent prefix collisions.
    - Dotted/unquoted form (e.g. ``cat.sch.t``): requires the match NOT be
      immediately followed by an identifier character (``[A-Za-z0-9_]``) so
      ``orders`` does not match inside ``orders_2024``.

    A view referencing any not-migrated object is cascade-skipped (finding #19)
    rather than hard-failing with TABLE_OR_VIEW_NOT_FOUND.
    """
    own_dotted = view_fqn.strip("`").replace("`.`", ".")

    for fqn in not_migrated_names:
        dotted = fqn.strip("`").replace("`.`", ".")
        if dotted == own_dotted:
            continue
        if fqn in ddl:
            return fqn
        if re.search(re.escape(dotted) + r"(?![A-Za-z0-9_])", ddl):
            return fqn
    return None


# COMMAND ----------
# Migrate a single view


def migrate_view(
    view_info: dict,
    *,
    config: MigrationConfig,
    auth: AuthManager,
    tracker: TrackingManager,
    explorer: CatalogExplorer,
    wh_id: str,
    not_migrated_names: set[str] | None = None,
) -> dict:
    """Migrate a single view to the target workspace.

    If the view references an object that was not migrated (finding #19),
    record ``skipped_dependency_not_migrated`` and do not execute the DDL.
    """
    obj_name = view_info["object_name"]

    tracker.append_migration_status(
        [
            {
                "object_name": obj_name,
                "object_type": "view",
                "status": "in_progress",
                "error_message": None,
                "job_run_id": None,
                "task_run_id": None,
                "source_row_count": None,
                "target_row_count": None,
                "duration_seconds": None,
            }
        ]
    )

    start = time.time()

    try:
        ddl = explorer.get_create_statement(obj_name)
    except Exception as exc:  # noqa: BLE001
        duration = time.time() - start
        return {
            "object_name": obj_name,
            "object_type": "view",
            "status": "failed",
            "error_message": f"Failed to get DDL: {exc}",
            "duration_seconds": duration,
        }

    dep = view_dependency_skip(obj_name, ddl, not_migrated_names or set())
    if dep is not None:
        return {
            "object_name": obj_name,
            "object_type": "view",
            "status": "skipped_dependency_not_migrated",
            "error_message": f"depends on not-migrated object {dep}",
            "duration_seconds": time.time() - start,
        }

    # Replace CREATE VIEW with CREATE OR REPLACE VIEW
    ddl = rewrite_ddl(ddl, r"CREATE\s+VIEW\b", "CREATE OR REPLACE VIEW")

    if config.dry_run:
        duration = time.time() - start
        logger.info("[DRY RUN] Would execute DDL for view %s", obj_name)
        return {
            "object_name": obj_name,
            "object_type": "view",
            "status": "skipped",
            "error_message": "dry_run",
            "duration_seconds": duration,
        }

    logger.info("Executing DDL for view %s", obj_name)
    result = execute_and_poll(auth, wh_id, ddl)
    duration = time.time() - start

    if result["state"] != "SUCCEEDED":
        return {
            "object_name": obj_name,
            "object_type": "view",
            "status": "failed",
            "error_message": result.get("error", result["state"]),
            "duration_seconds": duration,
        }

    return {
        "object_name": obj_name,
        "object_type": "view",
        "status": "validated",
        "error_message": None,
        "duration_seconds": duration,
    }


# COMMAND ----------
# Notebook execution


def run(dbutils, spark) -> None:
    """Entry point when running as a Databricks notebook."""
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    spark_session = spark
    tracker = TrackingManager(spark_session, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)
    explorer = CatalogExplorer(spark_session, auth)

    # Parse view list from task values
    view_list_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="view_list")
    views_raw: list[dict] = json.loads(view_list_json)
    logger.info("Received %d views to migrate.", len(views_raw))

    # Build ordered list using dependency resolution
    view_fqns = [v["object_name"] for v in views_raw]
    ordered_fqns = explorer.resolve_view_dependency_order(view_fqns)

    # Build a lookup for view info
    view_lookup: dict[str, dict] = {v["object_name"]: v for v in views_raw}

    logger.info("Dependency order resolved. Processing %d views.", len(ordered_fqns))

    wh_id = find_warehouse(auth)

    # Objects whose latest migration_status is not 'validated' — a view
    # referencing any of these is cascade-skipped (finding #19).
    not_migrated_names = tracker.not_validated_object_names(source_type="uc")

    # Process views in dependency order. view_table_usage does not exist in
    # UC, so topological sort is best-effort (parsed from view_definition) —
    # any missed dependency edges are caught by the retry loop below: if a
    # view fails with TABLE_OR_VIEW_NOT_FOUND on pass N, its upstream may land
    # on pass N+1. Stop when a full pass produces no additional successes.

    pending_fqns: list[str] = list(ordered_fqns)
    final_by_fqn: dict[str, dict] = {}

    for pass_num in range(1, _MAX_RETRY_PASSES + 1):
        next_pending: list[str] = []
        pass_progress = False
        for fqn in pending_fqns:
            view_info = view_lookup.get(fqn)
            if view_info is None:
                logger.warning("View %s not in input list, skipping.", fqn)
                continue
            try:
                res = migrate_view(
                    view_info,
                    config=config,
                    auth=auth,
                    tracker=tracker,
                    explorer=explorer,
                    wh_id=wh_id,
                    not_migrated_names=not_migrated_names,
                )
            except Exception as exc:  # noqa: BLE001
                res = {
                    "object_name": fqn,
                    "object_type": "view",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": 0.0,
                }
            if res["status"] == "validated":
                pass_progress = True
                final_by_fqn[fqn] = res
            elif res["status"] in ("skipped", "skipped_dependency_not_migrated"):
                # dry_run or a cascade-skip (finding #19) — terminal, no retry.
                # A cascade-skipped view becomes a not-migrated dependency for
                # later views in dependency order (transitive cascade).
                final_by_fqn[fqn] = res
                if res["status"] == "skipped_dependency_not_migrated":
                    not_migrated_names.add(fqn)
            else:
                # failed — possibly missing upstream. Keep the last attempt
                # recorded but try again on next pass.
                final_by_fqn[fqn] = res
                next_pending.append(fqn)
            logger.info(
                "View %s -> %s (pass %d)",
                res["object_name"],
                res["status"],
                pass_num,
            )
        if not next_pending:
            break
        if not pass_progress:
            logger.warning(
                "No view made progress on pass %d; %d still failing, giving up.",
                pass_num,
                len(next_pending),
            )
            break
        pending_fqns = next_pending

    results = list(final_by_fqn.values())

    # Record final statuses

    tracker.append_migration_status(results)
    logger.info(
        "Views worker complete. %d succeeded, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "failed"),
    )


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
