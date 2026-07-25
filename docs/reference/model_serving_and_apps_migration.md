# Model Serving & Apps Migration

**Status:** guidance (empirically validated 2026-06-24 on the migration lab).
Companion to `docs/dlt_pipeline_migration.md`. Like DLT, these surfaces are migrated
with the **Databricks Terraform exporter** plus this tool's UC migration — not (yet) a
dedicated worker. This note records what the exporter does and does not do for each, the
working process, and the gotchas found during live testing.

## Decision rule (when to build a worker vs document)
Build a code worker when **either** (a) there is stateful **data** work the exporter
cannot do — Vector Search (re-index), Online Tables (re-sync), Lakeflow Connect
(clone+boundary+view), which are already built — **or** (b) there is significant
**reference-remapping / redeploy orchestration** the exporter botches. By this rule:

| Surface | Exporter (live result) | Verdict |
|---|---|---|
| **Model Serving** | full endpoint config **when READY** + wires to the model | document + coordinate (`models_worker` + apply endpoint HCL) |
| **Apps** | definition stub only — no source, no deployment | **build candidate** (recreate + remap resources + redeploy) |

---

## Model Serving

### What the exporter captures
With `-listing=model-serving`, for a **custom-model** endpoint, the exporter emits the
**full** `databricks_model_serving` config — `served_entities` (workload type/size,
`scale_to_zero_enabled`, `entity_version`) and `traffic_config` routes — and **wires the
endpoint to the model**: `entity_name = databricks_registered_model.<model>.id`, pulling
the registered model + schema + catalog in as dependencies.

### What it does NOT do
- **Foundation-model / pay-per-token endpoints are skipped** (`skipping endpoint … that
  is foundation model`). They are system-provided on every workspace, so they don't need
  migrating — they already exist on the target.
- The `databricks_registered_model` it emits is a **metadata shell** — name/schema/
  catalog/owner, **no versions, no artifacts**. Applying the exporter output *alone* would
  create a versionless model, and the endpoint create would then **fail** on the missing
  `entity_version`.

### Working process
1. **Migrate the model with this tool** — `models_worker` recreates the registered model
   and each version by **downloading the source version's artifacts (MLflow) and
   re-registering them on the target** (MLflow ingests them into target-managed storage),
   then sets aliases. This is what makes the served version exist + `READY` on the target.
   Requires `mlflow` on the serverless runtime.
2. **Export the endpoint** from the source with `-listing=model-serving` — **only when the
   endpoint is `READY`.** ⚠️ A `NOT_READY` (provisioning) endpoint exports as a *name-only
   stub*; wait for `READY` to get the full config.
3. **Align the version** — ensure the endpoint's `entity_version` matches the version the
   tool migrated (version numbers can differ on the target).
4. **Apply the endpoint HCL** to the target (point the provider at the target). With the
   model already migrated, the endpoint creates and provisions.
5. **Cut over** — blue-green: stand up the target endpoint, then move client traffic;
   endpoint URLs change.

---

## Apps

### What the exporter captures
With `-listing=apps`, the exporter emits only the app **definition** —
`resource "databricks_app" { name = … }` (plus `description` and `resources` if set).
**Even for a deployed, running app**, that is all: **no source code, no `app.yaml`/command,
no deployment, no compute config.** Applied to the target, the app is created but
`UNAVAILABLE` with `active_deployment: NONE` — a non-functional shell.

Reason: in the Terraform model, `databricks_app` manages only the app definition. The app's
**source code is workspace files** (a separate concern) and the **deployment** is a distinct
step (`databricks apps deploy`) that is not a Terraform resource at all.

### Working process (three separate things)
1. **Definition** — `apps create` (or apply the exporter's `databricks_app`), including
   `resources` (bound SQL warehouse / serving endpoint / secret / job / UC securable).
   **Remap each resource reference to its target equivalent** — the source IDs are dead on
   the target.
2. **Source code** — migrate the app's workspace files (exporter `wsfiles`/`repos`, or the
   workspace-asset migration).
3. **Deploy** — `databricks apps deploy <app> --source-code-path /Workspace/<path>` to
   produce an active deployment. (The path must be a `/Workspace/...` path.)

### Build candidate
Because the exporter does almost nothing here and there is genuine **resource-reference
remapping + redeploy orchestration**, a `migrate_apps` worker is a reasonable future build
(discover via the existing `StatefulExplorer.list_apps` → recreate definition → remap
resources to target IDs → ensure source present → `apps.deploy` → validate). Shape mirrors
the Vector Search / Lakeflow Connect workers.

---

## Gotchas found during testing
- **Export serving endpoints only when `READY`** — `NOT_READY` → name-only stub.
- **Registering a UC model needs a UC-enabled cluster** — a No-Isolation / singleNode
  cluster fails with `PERMISSION_DENIED: Access denied to clusters that don't have Unity
  Catalog enabled`. Use `data_security_mode = SINGLE_USER`.
- **Serverless job base env lacks `mlflow`** (`ModuleNotFoundError`) — add
  `%pip install mlflow` (or use an ML runtime on classic compute).
- **App `--source-code-path` must be a `/Workspace/...` path**, not `/Users/...`.
- **Foundation-model endpoints are auto-skipped** by the exporter (and don't need
  migrating).
