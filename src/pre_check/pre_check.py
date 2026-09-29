# Databricks notebook source

# COMMAND ----------

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

# Pre-Check: validate connectivity, permissions, and prerequisites before migration.

from common.auth import AuthManager
from common.catalog_utils import CatalogExplorer
from common.config import MigrationConfig
from common.tracking import TrackingManager
from pre_check.collision_detection import (
    build_skip_status_rows,
    detect_collisions,
    unprobed_types_present,
)

# COMMAND ----------


def _list_shares(client):
    """SDK-compat shim: SharesAPI.list() was renamed to list_shares() in
    databricks-sdk 0.6x. Prefer the new name, fall back to the old (finding
    #5). Mirrors catalog_utils.list_shares so the sharing preflight actually
    exercises the API instead of always WARNing on AttributeError."""
    lister = getattr(client.shares, "list_shares", None) or client.shares.list
    return list(lister())


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]  # noqa: F821
        return True
    except NameError:
        return False


def missing_required_mounts(target_mounts: list[str], required_mounts: list[str]) -> list[str]:
    """Return required mount names absent from the target's mounts.

    Normalizes target entries to their bare ``<name>`` (accepts ``/mnt/x``,
    ``/mnt/x/`` or ``x``). /mnt-backed Hive tables can't be recreated on the
    target until the operator recreates the mount there.
    """
    norm = set()
    for m in target_mounts:
        s = (m or "").strip().strip("/")
        if s.startswith("mnt/"):
            s = s[len("mnt/"):]
        s = s.split("/", 1)[0]
        if s:
            norm.add(s)
    return [r for r in required_mounts if r not in norm]


def target_dbfs_root_probe_verdict(result: dict) -> tuple[str, str]:
    """Turn a target-warehouse DBFS-root probe result into a (status, message).

    The DBFS-root two-hop creates MANAGED tables in the TARGET's own DBFS root
    (``CREATE DATABASE`` / ``CREATE TABLE`` in the target ``hive_metastore``).
    The probe runs that on the target warehouse; this maps its outcome:

    - SUCCEEDED → PASS.
    - FAILED with the ``DBFS_DISABLED`` signature (public DBFS root disabled on
      the target) → FAIL: the DBFS-root leg cannot run. This is the exact error
      the orchestrator hits mid-migration, surfaced at preflight instead.
    - Any other FAILED → WARN (don't mask a real disabled-root behind a
      transient warehouse error, but don't false-PASS either).
    """
    if result.get("state") == "SUCCEEDED":
        return "PASS", "Target DBFS root is writable."
    err = str(result.get("error", result.get("state", "")) or "")
    if "DBFS_DISABLED" in err or "Public DBFS root is disabled" in err:
        return (
            "FAIL",
            f"Target DBFS root is disabled — the DBFS-root two-hop cannot create "
            f"managed tables in the target hive_metastore: {err[:200]}",
        )
    return "WARN", f"Could not confirm target DBFS-root writability: {err[:200]}"


def target_warehouse_dac_probe_verdict(result: dict) -> tuple[str, str]:
    """Turn a target-warehouse abfss-write probe result into a (status, message).

    The DBFS-root two-hop STAGE-2 and ADLS-backed Hive migration create/read
    ``abfss://`` locations through the TARGET SQL warehouse, which needs a Data
    Access Config (``fs.azure.account.key`` or an OAuth service principal) for
    the staging storage account. The default-DBFS-root database probe only
    created a *database* in the target's DEFAULT DBFS root — that path never
    touches the account key — so an empty DAC passed green and the migration then
    failed mid-run on ``fs.azure.account.key``. This probe creates a table at
    an ``abfss`` LOCATION on the target warehouse; this maps its outcome:

    - SUCCEEDED → PASS.
    - FAILED with the missing/invalid account-key signature → FAIL: surface the
      exact ``fs.azure.account.key`` error the orchestrator hits mid-migration at
      preflight instead, with the fix (configure the target warehouse DAC).
    - Any other FAILED → WARN (don't false-PASS, but don't mask a real gap
      behind a transient warehouse error either).
    """
    if result.get("state") == "SUCCEEDED":
        return "PASS", "Target warehouse can read/write the abfss staging location."
    err = str(result.get("error", result.get("state", "")) or "")
    if (
        "fs.azure.account.key" in err
        or "Failure to initialize configuration for storage account" in err
    ):
        return (
            "FAIL",
            "Target warehouse Data Access Config is missing the storage account key "
            "for the abfss staging location — ADLS-backed Hive migration and the "
            f"DBFS-root two-hop will fail mid-run: {err[:200]}",
        )
    return "WARN", f"Could not confirm target warehouse abfss access: {err[:200]}"


# COMMAND ----------


def run(dbutils, spark):  # noqa: D103
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    explorer = CatalogExplorer(spark, auth)

    tracker.init_tracking_tables()

    results: list[dict] = []

    def _add(check_name: str, status: str, message: str, action_required: str = "") -> None:
        results.append(
            {
                "check_name": check_name,
                "status": status,
                "message": message,
                "action_required": action_required,
            }
        )

    # 1. check_source_auth
    try:
        connectivity = auth.test_connectivity()
        if connectivity["source"]:
            _add("check_source_auth", "PASS", "Source workspace authentication succeeded.")
        else:
            _add(
                "check_source_auth",
                "FAIL",
                "Source workspace authentication failed.",
                "Verify SPN credentials and source workspace URL.",
            )
    except Exception as e:
        _add(
            "check_source_auth",
            "FAIL",
            f"Source auth error: {e}",
            "Verify SPN credentials and source workspace URL.",
        )

    # 2. check_target_auth
    try:
        if connectivity["target"]:
            _add("check_target_auth", "PASS", "Target workspace authentication succeeded.")
        else:
            _add(
                "check_target_auth",
                "FAIL",
                "Target workspace authentication failed.",
                "Verify SPN credentials and target workspace URL.",
            )
    except Exception as e:
        _add(
            "check_target_auth",
            "FAIL",
            f"Target auth error: {e}",
            "Verify SPN credentials and target workspace URL.",
        )

    # 3. check_source_metastore
    try:
        row = spark.sql("SELECT current_metastore() AS ms").first()
        _add("check_source_metastore", "PASS", f"Source metastore: {row.ms}")
    except Exception as e:
        _add(
            "check_source_metastore",
            "FAIL",
            f"Cannot query source metastore: {e}",
            "Ensure the workspace is attached to a Unity Catalog metastore.",
        )

    # 4. check_target_metastore
    try:
        ms = auth.target_client.metastores.summary()
        _add("check_target_metastore", "PASS", f"Target metastore: {ms.name}")
    except Exception as e:
        _add(
            "check_target_metastore",
            "FAIL",
            f"Cannot reach target metastore: {e}",
            "Ensure target workspace has a UC metastore assigned.",
        )

    # 4b. check_deploy_is_source (guards user_guide #3: the bundle must be
    # deployed to the SOURCE workspace. The jobs run local Spark against the
    # deploy workspace and treat it as the source; deploying to the target
    # makes discovery scan the empty target -> silent no-op. Compare the
    # current (deploy) workspace URL to the configured source/target URLs.
    # Only FAILs on the clear misconfiguration (deploy == target); passes or
    # skips otherwise so same-metastore migrations aren't false-flagged.
    def _host(url: str) -> str:
        return (url or "").strip().rstrip("/").split("://")[-1].lower()

    try:
        current_host = _host(spark.conf.get("spark.databricks.workspaceUrl"))  # noqa: F821
        src_host = _host(config.source_workspace_url)
        tgt_host = _host(config.target_workspace_url)
        if current_host and current_host == tgt_host and tgt_host != src_host:
            _add(
                "check_deploy_is_source",
                "FAIL",
                f"Bundle appears deployed to the TARGET workspace ({current_host}). "
                f"It must be deployed to the SOURCE ({src_host}) — the jobs inventory "
                f"the deploy workspace as the source.",
                "Redeploy the bundle to the source workspace (user_guide Step 6) "
                "and store the SPN secret there (Step 3).",
            )
        elif current_host and current_host == src_host:
            _add("check_deploy_is_source", "PASS", f"Deploy workspace is the source ({src_host}).")
        else:
            _add(
                "check_deploy_is_source",
                "WARN",
                f"Could not confirm deploy workspace matches source "
                f"(current={current_host!r}, source={src_host!r}).",
            )
    except Exception as e:  # noqa: BLE001
        _add("check_deploy_is_source", "WARN", f"Could not resolve deploy workspace URL: {e}")

    # 5. check_source_sharing
    try:
        _list_shares(auth.source_client)
        _add("check_source_sharing", "PASS", "Source Delta Sharing provider is accessible.")
    except Exception as e:
        _add(
            "check_source_sharing",
            "WARN",
            f"Source sharing check failed: {e}",
            "Delta Sharing may not be enabled on source workspace.",
        )

    # 6. check_target_sharing
    try:
        _list_shares(auth.target_client)
        _add("check_target_sharing", "PASS", "Target Delta Sharing provider is accessible.")
    except Exception as e:
        _add(
            "check_target_sharing",
            "WARN",
            f"Target sharing check failed: {e}",
            "Delta Sharing may not be enabled on target workspace.",
        )

    # 7. check_catalog_filter
    try:
        available = explorer.list_catalogs()
        if config.catalog_filter:
            missing = [c for c in config.catalog_filter if c not in available]
            if missing:
                _add(
                    "check_catalog_filter",
                    "WARN",
                    f"Catalogs listed in catalog_filter not found on source: {missing}. Discovery will skip them.",
                    "If this is unexpected, check catalog_filter for typos. "
                    "Safe to ignore when running Hive-only or partial migrations.",
                )
            else:
                _add(
                    "check_catalog_filter",
                    "PASS",
                    f"All filtered catalogs exist: {config.catalog_filter}",
                )
        else:
            _add(
                "check_catalog_filter",
                "PASS",
                f"No catalog filter set. Found {len(available)} catalogs.",
            )
    except Exception as e:
        _add(
            "check_catalog_filter",
            "FAIL",
            f"Cannot list catalogs: {e}",
            "Ensure SPN has catalog-level permissions on source.",
        )

    # 8. check_storage_credentials
    try:
        creds = list(auth.target_client.storage_credentials.list())
        _add(
            "check_storage_credentials",
            "PASS",
            f"Target has {len(creds)} storage credential(s).",
        )
    except Exception as e:
        _add(
            "check_storage_credentials",
            "WARN",
            f"Cannot list storage credentials: {e}",
            "SPN may lack permission to list storage credentials on target.",
        )

    # 9. check_external_locations
    try:
        locs = list(auth.target_client.external_locations.list())
        _add(
            "check_external_locations",
            "PASS",
            f"Target has {len(locs)} external location(s).",
        )
    except Exception as e:
        _add(
            "check_external_locations",
            "WARN",
            f"Cannot list external locations: {e}",
            "SPN may lack permission to list external locations on target.",
        )

    # 10. check_tracking_schema
    try:
        spark.sql(f"DESCRIBE SCHEMA {config.tracking_catalog}.{config.tracking_schema}")
        _add(
            "check_tracking_schema",
            "PASS",
            f"Tracking schema {config.tracking_catalog}.{config.tracking_schema} exists.",
        )
    except Exception as e:
        _add(
            "check_tracking_schema",
            "FAIL",
            f"Tracking schema not found: {e}",
            "Run init_tracking_tables or check tracking_catalog/tracking_schema params.",
        )

    # ----- Hive Metastore checks (Phase 2) -----

    # 11. check_hive_metastore_accessible
    try:
        dbs = spark.sql("SHOW DATABASES IN hive_metastore").collect()
        _add(
            "check_hive_metastore_accessible",
            "PASS",
            f"Hive metastore reachable; {len(dbs)} database(s) found.",
        )
    except Exception as e:
        _add(
            "check_hive_metastore_accessible",
            "WARN",
            f"hive_metastore not accessible: {e}",
            "If the source has no Hive data, ignore. Otherwise verify the source workspace has hive_metastore enabled.",
        )

    # 12. check_hive_dbfs_root_config — surfaces DBFS-root tables and flags
    # config requirements for the Hive DBFS-root worker.
    try:
        hive_explorer = CatalogExplorer(spark, auth)
        dbfs_root_tables: list[str] = []
        for db in hive_explorer.list_hive_databases():
            for tbl in hive_explorer.classify_hive_tables(db):
                if tbl["data_category"] == "hive_managed_dbfs_root":
                    dbfs_root_tables.append(tbl["fqn"])
        if not dbfs_root_tables:
            _add(
                "check_hive_dbfs_root_config",
                "PASS",
                "No Hive DBFS-root managed tables found — migrate_hive_dbfs_root can stay false.",
            )
        elif not config.migrate_hive_dbfs_root:
            _add(
                "check_hive_dbfs_root_config",
                "WARN",
                f"{len(dbfs_root_tables)} Hive DBFS-root managed table(s) discovered; "
                f"migration skipped because migrate_hive_dbfs_root=false.",
                "Review the discovered list; set migrate_hive_dbfs_root=true "
                "and hive_dbfs_staging_path in config.yaml to include them.",
            )
        elif not config.hive_dbfs_staging_path:
            _add(
                "check_hive_dbfs_root_config",
                "FAIL",
                f"{len(dbfs_root_tables)} DBFS-root table(s) selected for migration but "
                f"hive_dbfs_staging_path is empty.",
                "Set hive_dbfs_staging_path in config.yaml to a shared abfss staging "
                "location both workspaces can reach (e.g. abfss://hive@acct.dfs.core.windows.net/stage/); "
                "the tables are staged there, then re-created as managed tables in the target's own DBFS root.",
            )
        else:
            # Probe write to the configured staging path
            from datetime import datetime

            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            probe_path = config.hive_dbfs_staging_path.rstrip("/") + f"/.precheck_probe_{ts}"
            try:
                spark.createDataFrame([(1,)], "x INT").write.mode("overwrite").format("delta").save(  # type: ignore[attr-defined]
                    probe_path
                )
                # Clean up
                dbutils.fs.rm(probe_path, True)  # type: ignore[attr-defined] # noqa: F821
                _add(
                    "check_hive_dbfs_root_config",
                    "PASS",
                    f"{len(dbfs_root_tables)} DBFS-root table(s) will be staged via "
                    f"{config.hive_dbfs_staging_path} then re-created managed on target; write probe passed.",
                )
            except Exception as probe_exc:  # noqa: BLE001
                _add(
                    "check_hive_dbfs_root_config",
                    "FAIL",
                    f"Cannot write to hive_dbfs_staging_path {config.hive_dbfs_staging_path}: {probe_exc}",
                    "Verify the SPN has write access to this ADLS staging location, and that "
                    "a storage credential + external location exists on both workspaces.",
                )
    except Exception as e:
        _add(
            "check_hive_dbfs_root_config",
            "WARN",
            f"Could not enumerate Hive tables: {e}",
            "If hive_metastore is not in use, this is safe to ignore.",
        )

    # 13. check_external_hive_metastore — surface clusters & jobs that reference
    # an external Hive metastore (customer-managed MySQL/Azure SQL). These need
    # manual reconfiguration on target; see docs/external_hive_metastore.md.
    try:
        ws = auth.source_client
        ext_ms_keys = (
            "javax.jdo.option.ConnectionURL",
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            "spark.sql.hive.metastore.jars",
            "spark.sql.hive.metastore.version",
        )
        hits: list[str] = []
        for c in ws.clusters.list():
            conf = getattr(c, "spark_conf", None) or {}
            if any(k in conf for k in ext_ms_keys):
                hits.append(f"cluster:{c.cluster_name or c.cluster_id}")
        for w in ws.warehouses.list():
            conf = getattr(w, "spark_confs", None) or {}
            if any(k in conf for k in ext_ms_keys):
                hits.append(f"warehouse:{w.name}")
        if hits:
            _add(
                "check_external_hive_metastore",
                "WARN",
                f"External Hive metastore referenced by {len(hits)} compute resource(s): {hits[:10]}"
                + ("..." if len(hits) > 10 else ""),
                "See docs/external_hive_metastore.md — customer must recreate "
                "clusters/warehouses on target with the same metastore config.",
            )
        else:
            _add(
                "check_external_hive_metastore",
                "PASS",
                "No external Hive metastore references found on source clusters or warehouses.",
            )
    except Exception as e:
        _add(
            "check_external_hive_metastore",
            "WARN",
            f"Could not scan compute for external metastore config: {e}",
            "Manually check clusters/warehouses for javax.jdo.option.ConnectionURL in spark_conf.",
        )

    # ----- Target pre-existing state / collision detection (Check 14, X.4) -----
    #
    # Queries the target workspace for every object already recorded in
    # discovery_inventory. When the target has the same FQN AND there's
    # no migration_status row keyed by that FQN (meaning we didn't create
    # it), we emit a target_collision pre_check row. Policy:
    #
    #   on_target_collision=fail (default): each collision is a FAIL row.
    #     The orchestrator's pre_check gate refuses to start migrate when
    #     any FAIL target_collision row is in the latest pre_check_results.
    #
    #   on_target_collision=skip: each collision is a WARN row, and
    #     pre_check seeds a ``skipped_target_exists`` migration_status
    #     row so the worker on next migrate run short-circuits that
    #     object and leaves the target copy untouched.
    #
    # This check is a no-op on a fresh install (before discovery runs)
    # because discovery_inventory is empty.
    try:
        discovery_rows = spark.sql(
            f"""
            SELECT object_name, object_type, source_type
            FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory
            """
        ).collect()
        discovery_dicts = [
            {
                "object_name": r.object_name,
                "object_type": r.object_type,
                "source_type": r.source_type,
            }
            for r in discovery_rows
        ]
        status_rows = spark.sql(
            f"""
            SELECT DISTINCT object_name, object_type
            FROM {config.tracking_catalog}.{config.tracking_schema}.migration_status
            """
        ).collect()
        existing_status_keys: set[tuple[str, str]] = {
            (r.object_type, r.object_name) for r in status_rows
        }
        collisions = detect_collisions(
            target_client=auth.target_client,
            discovery_rows=discovery_dicts,
            existing_status_keys=existing_status_keys,
        )
        if not discovery_dicts:
            _add(
                "check_target_collisions",
                "PASS",
                "discovery_inventory is empty — run discovery before re-running "
                "pre-check to enable collision detection.",
            )
        elif not collisions:
            _add(
                "check_target_collisions",
                "PASS",
                f"Target has no pre-existing object among {len(discovery_dicts)} discovered source objects.",
            )
        else:
            # Fail CLOSED (review finding #10): records the probe couldn't
            # verify (PermissionDenied / transient) are surfaced as a FAIL
            # regardless of policy — we must never proceed against a target we
            # couldn't confirm is clear, and must never silently skip it.
            check_failures = [c for c in collisions if c.get("check_failed")]
            collisions = [c for c in collisions if not c.get("check_failed")]
            if check_failures:
                cf_sample = [
                    f"{c['object_type']} {c['target_fqn']}: {c.get('error', 'unknown error')}"
                    for c in check_failures[:5]
                ]
                _add(
                    "check_target_collisions",
                    "FAIL",
                    f"{len(check_failures)} target object(s) could not be collision-checked "
                    f"(probe errored — failing closed). Sample: {cf_sample}",
                    "The migration SPN likely lacks read access on the target, or a "
                    "transient error occurred. Grant the SPN read on the target objects "
                    "and re-run pre-check; do NOT migrate until collision detection completes.",
                )
        if collisions:
            policy = (config.on_target_collision or "fail").lower()
            grouped: dict[str, list[str]] = {}
            for c in collisions:
                grouped.setdefault(c["object_type"], []).append(c["target_fqn"])
            summary = ", ".join(f"{otype}={len(v)}" for otype, v in sorted(grouped.items()))
            # First few target FQNs surfaced in the message — capped so the
            # pre_check_results table doesn't balloon to multi-KB rows.
            sample = [c["target_fqn"] for c in collisions[:10]]
            details = f"{len(collisions)} collision(s) — {summary}. Sample: {sample}"
            if policy == "skip":
                _add(
                    "check_target_collisions",
                    "WARN",
                    f"{details} — on_target_collision=skip; these will be skipped (skipped_target_exists).",
                    "Target objects are left untouched. Verify that the pre-existing objects are the intended state.",
                )
                skip_rows = build_skip_status_rows(collisions)
                if skip_rows:
                    tracker.append_migration_status(skip_rows)
            else:  # fail (the default)
                _add(
                    "check_target_collisions",
                    "FAIL",
                    f"{details} — on_target_collision=fail; migrate workflow will refuse to start.",
                    "Rename, drop, or move the colliding target object(s); or set "
                    "on_target_collision=skip to proceed without overwriting them.",
                )
        # Surface coverage (review finding #6): name the in-scope object types
        # that were NOT collision-checked, so a clean result isn't misread as
        # "everything is safe".
        unprobed = unprobed_types_present(discovery_dicts)
        if unprobed:
            _add(
                "check_collision_coverage",
                "PASS",
                f"NOT collision-checked: {', '.join(unprobed)}. These target object "
                "types were not probed for pre-existence (see collision_detection "
                "._NOT_PROBED_TYPES for the rationale per type); verify manually if "
                "reusing a populated target.",
            )
    except Exception as e:  # noqa: BLE001 — discovery_inventory absent / transient
        # discovery_inventory / migration_status may not exist yet on a
        # first-ever pre_check run. init_tracking_tables above created
        # them, but the collision check still needs discovery to have
        # populated discovery_inventory at least once. Surface as a WARN
        # so the operator sees why collision detection was skipped.
        _add(
            "check_target_collisions",
            "WARN",
            f"Collision detection skipped: {e}",
            "Run discovery before re-running pre-check to enable collision detection.",
        )

    # 14b. check_target_dbfs_root — the DBFS-root two-hop lands MANAGED tables in
    # the TARGET's own DBFS root (CREATE DATABASE / CREATE TABLE in the target
    # hive_metastore). Probe the TARGET via its warehouse — NOT dbutils.fs,
    # which runs on the SOURCE (deploy) workspace and gave a false PASS while
    # the target's DBFS root was disabled. A create-then-drop of a throwaway
    # database reproduces the exact DBFS_DISABLED error the orchestrator hits.
    try:
        if not config.migrate_hive_dbfs_root:
            _add("check_target_dbfs_root", "PASS", "No DBFS-root migration requested (migrate_hive_dbfs_root=false).")
        else:
            from common.sql_utils import execute_and_poll, find_warehouse

            _t_wh = find_warehouse(auth)  # defaults to the TARGET workspace
            ts = __import__("time").strftime("%H%M%S")
            probe_db = f"hive_metastore.__wsm_dbfs_root_probe_{ts}"
            res = execute_and_poll(auth, _t_wh, f"CREATE DATABASE IF NOT EXISTS {probe_db}")
            status, message = target_dbfs_root_probe_verdict(res)
            # Best-effort cleanup if it did get created.
            if res.get("state") == "SUCCEEDED":
                execute_and_poll(auth, _t_wh, f"DROP DATABASE IF EXISTS {probe_db}")
            action = (
                "Enable the DBFS root on the target workspace (required for the "
                "DBFS-root two-hop copy), or set migrate_hive_dbfs_root=false."
                if status == "FAIL"
                else ""
            )
            _add("check_target_dbfs_root", status, message, action)
    except Exception as e:  # noqa: BLE001
        _add("check_target_dbfs_root", "WARN", f"Could not probe target DBFS root: {e}")

    # 14b-2 (#25). check_target_warehouse_dac — the DBFS-root two-hop STAGE-2 and
    # ADLS-backed Hive migration write/read abfss:// through the TARGET warehouse,
    # which needs a Data Access Config (fs.azure.account.key / OAuth). The default
    # DBFS-root database probe never touches the account key, so an empty DAC
    # passed green and migration died mid-run. Probe migrate_hive_dbfs_root here.
    try:
        if not config.migrate_hive_dbfs_root or not config.hive_dbfs_staging_path:
            _add(
                "check_target_warehouse_dac",
                "PASS",
                "abfss-backed DBFS-root Hive migration not requested — DAC not required.",
            )
        else:
            from common.sql_utils import execute_and_poll, find_warehouse

            _t_wh = find_warehouse(auth)  # defaults to the TARGET workspace
            ts = __import__("time").strftime("%H%M%S")
            probe_loc = config.hive_dbfs_staging_path.rstrip("/") + f"/.wsm_dac_probe_{ts}"
            probe_tbl = f"hive_metastore.default.__wsm_dac_probe_{ts}"
            res = execute_and_poll(
                auth, _t_wh, f"CREATE TABLE {probe_tbl} (x INT) USING DELTA LOCATION '{probe_loc}'"
            )
            status, message = target_warehouse_dac_probe_verdict(res)
            # Best-effort cleanup if the table did get created.
            if res.get("state") == "SUCCEEDED":
                execute_and_poll(auth, _t_wh, f"DROP TABLE IF EXISTS {probe_tbl}")
            action = (
                "Configure the target SQL warehouse Data Access Config with the storage "
                "account key or an OAuth service principal for the staging account "
                "(e.g. spark.hadoop.fs.azure.account.key.<acct>.dfs.core.windows.net="
                "{{secrets/<scope>/<key>}}), then restart the warehouse."
                if status == "FAIL"
                else ""
            )
            _add("check_target_warehouse_dac", status, message, action)
    except Exception as e:  # noqa: BLE001
        _add("check_target_warehouse_dac", "WARN", f"Could not probe target warehouse abfss access: {e}")

    # 14c. check_target_mounts — every /mnt mount required by a /mnt-backed Hive
    # table (mount_prerequisite markers from discovery) must already exist on the
    # target; the tool never recreates mounts or touches mount credentials.
    try:
        marker_rows = spark.sql(
            f"""
            SELECT metadata_json FROM {config.tracking_catalog}.{config.tracking_schema}.discovery_inventory
            WHERE object_type = 'mount_prerequisite'
            """
        ).collect()
        import json as _json
        required = sorted({_json.loads(r.metadata_json).get("mount") for r in marker_rows if r.metadata_json})
        required = [m for m in required if m]
        if not required:
            _add("check_target_mounts", "PASS", "No /mnt-backed Hive tables require a target mount.")
        else:
            target_mounts = [m.mountPoint for m in dbutils.fs.mounts()]  # type: ignore[attr-defined]  # noqa: F821
            missing = missing_required_mounts(target_mounts, required)
            if missing:
                _add(
                    "check_target_mounts", "FAIL",
                    f"Required target mount(s) missing: {missing}",
                    "Recreate the listed /mnt mount(s) on the target workspace before "
                    "migrating the /mnt-backed Hive tables (see the dashboard's mount "
                    "prerequisites panel).",
                )
            else:
                _add("check_target_mounts", "PASS", f"All {len(required)} required target mount(s) present.")
    except Exception as e:  # noqa: BLE001
        _add(
            "check_target_mounts", "WARN",
            f"Could not verify target mounts: {e}",
            "Run discovery first so mount_prerequisite markers exist, then re-run pre_check.",
        )

    # 14d. check_staging_path_reachable — best-effort ls of the shared abfss
    # staging area used by the DBFS-root two-hop copy.
    try:
        if not config.migrate_hive_dbfs_root:
            _add("check_staging_path_reachable", "PASS", "DBFS-root migration disabled; staging path not needed.")
        elif not config.hive_dbfs_staging_path:
            _add(
                "check_staging_path_reachable", "FAIL",
                "hive_dbfs_staging_path is empty but DBFS-root migration is enabled.",
                "Set hive_dbfs_staging_path to a shared abfss:// location both workspaces can reach.",
            )
        else:
            try:
                dbutils.fs.ls(config.hive_dbfs_staging_path)  # type: ignore[attr-defined]  # noqa: F821
                _add("check_staging_path_reachable", "PASS", f"Staging path reachable: {config.hive_dbfs_staging_path}")
            except Exception as se:  # noqa: BLE001
                _add(
                    "check_staging_path_reachable", "WARN",
                    f"Could not list staging path {config.hive_dbfs_staging_path}: {se}",
                    "Verify the SPN can read/write the shared staging container from both workspaces.",
                )
    except Exception as e:  # noqa: BLE001
        _add("check_staging_path_reachable", "WARN", f"Staging path check skipped: {e}")

    # Persist results
    tracker.append_pre_check_results(results)

    # Print summary table
    print(f"\n{'Check':<30} {'Status':<8} {'Message'}")
    print("-" * 100)
    for r in results:
        print(f"{r['check_name']:<30} {r['status']:<8} {r['message']}")
        if r["action_required"]:
            print(f"{'':>30} -> {r['action_required']}")
    print("-" * 100)

    fail_count = sum(1 for r in results if r["status"] == "FAIL")
    warn_count = sum(1 for r in results if r["status"] == "WARN")
    print(f"\nSummary: {len(results)} checks, {fail_count} FAIL, {warn_count} WARN")

    if fail_count > 0:
        msg = f"Pre-check failed: {fail_count} check(s) returned FAIL. See details above."
        raise Exception(msg)

    return results


# COMMAND ----------

if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
