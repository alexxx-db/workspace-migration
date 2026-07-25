# Databricks notebook source

# COMMAND ----------

from __future__ import annotations  # noqa: E402

import sys  # noqa: E402

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
    _nb = _ctx.notebookPath().get()
    _src = "/Workspace" + _nb.split("/files/")[0] + "/files/src"
    if _src not in sys.path:
        sys.path.insert(0, _src)
except NameError:
    pass

# COMMAND ----------
# Registered Models Worker (Phase 3 Task 34).
#
# Per model:
#   (a) create the registered-model shell on target via the SDK
#       (``registered_models.create``), and
#   (b) for each source version: DOWNLOAD its artifacts to local disk via
#       the SOURCE registry (``_download_source_artifacts``), then create
#       the target version via MLflow's ``create_model_version(name,
#       source=<local path>, run_id)`` (``_registry_client`` → target UC),
#       then apply that version's aliases against the TARGET version number
#       MLflow allocates.
#
# Why MLflow and not the SDK (finding #17): UC model versions are created
# through MLflow, not ``databricks.sdk``. ``ModelVersionsAPI`` has no
# ``create`` method (only delete/get/get_by_alias/list/update), so the
# previous ``client.model_versions.create(...)`` raised AttributeError on
# every version — the model migrated as an empty shell.
#
# Why stage-then-register (Option-2, proven live 2026-07-25): a direct
# ``create_model_version`` pointed at the target but reading the SOURCE
# metastore's ``abfss://unity-catalog@…`` storage FAILS — the target MLflow
# client falls back to DefaultAzureCredential, which has no creds on
# serverless ("Unable to download model artifacts … DefaultAzureCredential
# failed to retrieve a token"). So we download via the SOURCE registry
# first (UC vends creds for its own managed storage → local disk), then
# register on the target from that local path (MLflow uploads it into
# target-managed storage).
#
# Execution model: the worker runs on SOURCE compute. The default MLflow
# registry (``databricks-uc``) resolves against the source metastore for
# the download; ``_registry_client`` is pointed at the TARGET for the
# create — mirroring how ``auth.target_client`` targets the target for the
# SDK shell create. Download or create failures surface in error_message
# and mark the row ``validation_failed`` so a re-run retries.

import json
import logging
import time

from databricks.sdk.errors import AlreadyExists

from common.auth import AuthManager
from common.config import MigrationConfig
from common.tracking import TrackingManager
from migrate.reconciliation import resolve_current_job_run_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("models_worker")


def _is_notebook() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined] # noqa: F821
        return True
    except NameError:
        return False


def _parse_fqn(fqn: str) -> tuple[str, str, str]:
    parts = fqn.strip("`").split(".")
    if len(parts) != 3:
        raise ValueError(f"Malformed model FQN: {fqn}")
    return parts[0], parts[1], parts[2]


def _registry_client(auth: AuthManager):
    """Return an MLflow registry client pointed at the TARGET workspace's
    Unity Catalog model registry.

    The worker runs on source-workspace compute; aiming the MLflow client
    at the target registry is what makes ``create_model_version`` land the
    new version on the target (the MLflow analogue of
    ``auth.target_client``). ``mlflow`` is only present on the Databricks
    runtime, so it is imported lazily here — unit tests replace this whole
    function via the module seam and never import mlflow.
    """
    # The migration SPN authenticates via OAuth (client_id/secret), so
    # ``config.token`` is empty — building ``databricks://host:token`` from it
    # yields ``databricks`` (ambient = SOURCE runtime), and the create then
    # lands on / is denied by the source (proven live 2026-07-25:
    # "host=<source>, auth_type=runtime … does not have CREATE MODEL VERSION").
    # Vend a Bearer token from the target client's OAuth flow and set it as an
    # env-based Databricks profile so MLflow authenticates AS the target SPN.
    #
    # NOTE ordering (see apply_model): this SETS DATABRICKS_HOST/TOKEN to the
    # target. It must be called AFTER all source-side downloads, because the
    # downloads rely on ambient (source) runtime auth — env pointing at target
    # would send the download to the wrong workspace.
    import os

    from mlflow.tracking import MlflowClient

    host = auth.config.target_workspace_url
    headers = auth.target_client.config.authenticate()  # {"Authorization": "Bearer <token>"}
    bearer = headers.get("Authorization", "").split(" ", 1)[-1]
    os.environ["DATABRICKS_HOST"] = host
    os.environ["DATABRICKS_TOKEN"] = bearer
    return MlflowClient(tracking_uri="databricks", registry_uri="databricks-uc")


def _download_source_artifacts(auth: AuthManager, model_fqn: str, version: str) -> str:
    """Download a source model version's artifacts to a local path.

    Option-2 (stage-then-register): a target-pointed ``create_model_version``
    reading the SOURCE metastore's ``abfss://unity-catalog@…`` storage fails
    cross-metastore — the target client falls back to DefaultAzureCredential,
    which has no creds on serverless (proven live 2026-07-25). Instead we
    download the artifacts here via the SOURCE registry, where Unity Catalog
    vends credentials for its own managed storage, then hand the returned
    LOCAL path to the target registry's ``create_model_version`` (which
    uploads into target-managed storage).

    The worker runs on source-workspace compute, so MLflow's default registry
    (``databricks-uc``) already resolves against the source metastore.
    ``mlflow`` is runtime-only; imported lazily so unit tests inject this seam.
    """
    import mlflow

    mlflow.set_registry_uri("databricks-uc")
    return mlflow.artifacts.download_artifacts(artifact_uri=f"models:/{model_fqn}/{version}")


def apply_model(
    model: dict,
    *,
    auth: AuthManager,
    dry_run: bool,
) -> list[dict]:
    """Create the registered model + each version + each alias on target."""
    model_fqn = model.get("model_fqn", "")
    obj_key = f"MODEL_{model_fqn}"
    results: list[dict] = []

    start = time.time()
    if dry_run:
        results.append(
            {
                "object_name": obj_key,
                "object_type": "registered_model",
                "status": "skipped",
                "error_message": "dry_run",
                "duration_seconds": time.time() - start,
            }
        )
        return results

    try:
        catalog, schema, name = _parse_fqn(model_fqn)
    except ValueError as exc:
        results.append(
            {
                "object_name": obj_key,
                "object_type": "registered_model",
                "status": "failed",
                "error_message": str(exc),
                "duration_seconds": time.time() - start,
            }
        )
        return results

    client = auth.target_client
    # 1. Create the model shell
    try:
        client.registered_models.create(
            catalog_name=catalog,
            schema_name=schema,
            name=name,
            comment=model.get("comment"),
            storage_location=model.get("storage_location"),
        )
    except AlreadyExists:
        # IDEMPOTENT: model already exists, continue to versions + aliases
        pass
    except Exception as exc:  # noqa: BLE001
        # The runtime doesn't always map an existing-model error to the
        # AlreadyExists class — live it surfaced as a generic error
        # "Routine or Model '<name>' already exists". Treat any
        # already-exists message as idempotent (re-run) too.
        if "already exists" in str(exc).lower():
            pass
        else:
            results.append(
                {
                    "object_name": obj_key,
                    "object_type": "registered_model",
                    "status": "failed",
                    "error_message": str(exc),
                    "duration_seconds": time.time() - start,
                }
            )
            return results

    # Option-2 (stage-then-register), in two auth phases because MLflow auth
    # is global env state and download needs SOURCE creds / register needs
    # TARGET creds:
    #   Phase A — download every version's artifacts to local disk using the
    #     ambient (SOURCE) runtime auth. Do this BEFORE building the target
    #     registry client (which sets DATABRICKS_HOST/TOKEN to target env).
    #   Phase B — build the target registry client, then register each
    #     downloaded version FROM its local path.
    # A direct create_model_version from the source abfss:// URI fails
    # cross-metastore (finding #17, proven live 2026-07-25).
    full_name = f"{catalog}.{schema}.{name}"
    version_errors: list[str] = []

    # Phase A — source-side downloads (ambient source auth; no target env yet).
    staged: list[dict] = []  # {source_version, run_id, aliases, local_path}
    for v in model.get("versions", []):
        source_version = v.get("version", "?")
        # A version with no artifact location at all can't be moved
        # (external / GC'd artifacts — scenario E).
        artifact_source = v.get("storage_location") or v.get("source") or ""
        if not artifact_source:
            version_errors.append(
                f"v{source_version}: no artifact location (storage_location/source both empty); "
                "register manually"
            )
            continue
        try:
            local_path = _download_source_artifacts(auth, model_fqn, source_version)
        except Exception as exc:  # noqa: BLE001
            version_errors.append(
                f"v{source_version}: source artifact download failed "
                f"(models:/{model_fqn}/{source_version}): {exc}"
            )
            continue
        staged.append(
            {
                "source_version": source_version,
                "run_id": v.get("run_id"),
                "aliases": v.get("aliases") or [],
                "local_path": local_path,
            }
        )

    # Phase B — target registry client (sets target env), then register each
    # staged version. MLflow allocates its OWN target version numbers, so
    # aliases are applied against the RETURNED version, never the source one.
    registry = _registry_client(auth)
    versions_created = 0
    for s in staged:
        source_version = s["source_version"]
        local_path = s["local_path"]
        try:
            created = registry.create_model_version(
                name=full_name,
                source=local_path,
                run_id=s["run_id"],
            )
            versions_created += 1
        except AlreadyExists as exc:  # noqa: BLE001
            # Re-run without a preceding reconciliation drop — treat as done
            # but note it (reconciliation normally drops the whole model).
            version_errors.append(f"v{source_version}: already exists ({exc})")
            continue
        except Exception as exc:  # noqa: BLE001
            version_errors.append(
                f"v{source_version}: create_model_version failed (source={local_path}): {exc}"
            )
            continue

        # 3. Aliases — set against the TARGET version MLflow just allocated.
        target_version = int(created.version)
        for alias in s["aliases"]:
            try:
                client.registered_models.set_alias(
                    full_name=full_name,
                    alias=alias,
                    version_num=target_version,
                )
            except Exception as exc:  # noqa: BLE001
                version_errors.append(f"alias '{alias}' (src v{source_version}): {exc}")

    duration = time.time() - start
    status_msg_parts: list[str] = [f"{versions_created} version(s) created."]
    if version_errors:
        status_msg_parts.append("Errors: " + "; ".join(version_errors[:5]))
    # A version that failed to create/ingest leaves the model incomplete on
    # target — operator intervention required. Mark validation_failed
    # (non-terminal) so the next migrate run retries once access is granted.
    _is_failed = bool(version_errors)
    results.append(
        {
            "object_name": obj_key,
            "object_type": "registered_model",
            "status": "validation_failed" if _is_failed else "validated",
            "error_message": " ".join(status_msg_parts),
            "duration_seconds": duration,
        }
    )
    return results


def cleanup_partial_target(
    object_name: str, *, auth: AuthManager, spark=None, config=None
) -> None:
    """Drop the target registered model so the retry can recreate cleanly.

    C5: registered_model migration is multi-step (shell + N versions +
    artifact copies + aliases). When a run crashes between steps it
    leaves partial state on target (e.g. v1-v4 metadata present plus a
    half-copied v5 artifact directory). On retry, ``apply_model`` would
    happily tolerate ``AlreadyExists`` on the shell but leave the
    partial v5 unreconciled. Dropping the whole model lets the retry
    start clean.

    Best-effort: swallow ``NOT_FOUND`` (model was never created or
    crashed before the SDK call landed). Other errors propagate so the
    reconciler can log and continue resetting the row.

    Mirrors ``volume_worker.cleanup_partial_target``.
    """
    target_fqn = object_name
    # Discovery key is ``MODEL_<model_fqn>`` (see apply_model). Strip
    # the prefix so the SDK call sees the bare ``catalog.schema.name``.
    if target_fqn.startswith("MODEL_"):
        target_fqn = target_fqn[len("MODEL_") :]
    target_fqn = target_fqn.strip("`").replace("`.`", ".")
    try:
        auth.target_client.registered_models.delete(full_name=target_fqn)
        logger.info(
            "Reconciliation cleanup: dropped partial target registered model %s",
            target_fqn,
        )
    except Exception as exc:  # noqa: BLE001
        msg = str(exc).lower()
        if "not" in msg and ("exist" in msg or "found" in msg):
            logger.info(
                "Reconciliation cleanup: target model %s already absent; nothing to drop.",
                target_fqn,
            )
            return
        raise


def run(dbutils, spark) -> None:
    config = MigrationConfig.from_workspace_file()
    auth = AuthManager(config, dbutils)
    tracker = TrackingManager(spark, config)
    tracker.job_run_id = resolve_current_job_run_id(dbutils)

    rows_json = dbutils.jobs.taskValues.get(taskKey="orchestrator", key="registered_model_list")
    rows: list[dict] = json.loads(rows_json)
    logger.info("Received %d model records.", len(rows))

    results: list[dict] = []
    for r in rows:
        meta = r.get("metadata_json")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:  # noqa: BLE001
                continue
        if not isinstance(meta, dict):
            continue
        results.extend(apply_model(meta, auth=auth, dry_run=config.dry_run))

    if results:
        tracker.append_migration_status(results)
    logger.info(
        "Models worker complete. %d validated, %d validation_failed, %d failed.",
        sum(1 for r in results if r["status"] == "validated"),
        sum(1 for r in results if r["status"] == "validation_failed"),
        sum(1 for r in results if r["status"] == "failed"),
    )


if _is_notebook():
    run(dbutils, spark)  # type: ignore[name-defined]  # noqa: F821
