# Workspace Migration — User Guide

A Databricks Asset Bundle (DAB) that migrates Unity Catalog and legacy Hive
Metastore objects between Databricks workspaces, plus the surrounding
methodology for a full control-plane migration.

This guide covers both:

- **The tool** — how to set it up and run it, and exactly which objects it
  migrates (Sections 5–7).
- **The migration approach** — the phased methodology around the tool, the
  per-data-type strategy, and the assets the tool does *not* move directly
  (handled via the Terraform exporter or manual steps) — Sections 2–4, 8–11.

---

## 1. Overview

### What it does

`workspace-migration` replays a source workspace's Unity Catalog and Hive
Metastore objects into a target workspace. It moves data (managed-table
bytes), structure (catalogs, schemas, tables, views, functions, volumes), and
governance (grants, tags, row filters, column masks, customer shares) —
**without requiring data egress through external storage**.

It is one part of a larger migration. A control-plane migration also involves
infrastructure, identity, workspace assets (notebooks/jobs), and stateful
services — the tool handles the **UC & data plane**; the rest is covered by
the phased approach in Section 2 and the Terraform-exporter path in Section 8.

### When to use it

- **Control-plane migrations** — moving a workspace between regions or cloud
  account boundaries (e.g. UK West → UK South).
- **Account consolidations** — merging multiple Databricks accounts.
- **Region moves** — relocating workloads closer to users or data sources.
- **Workspace re-platforming** — onto a fresh workspace with updated
  networking, identity, or compute defaults.

A control-plane migration is **not** in-place: workspace IDs change, a new UC
metastore is required, and a new DBFS root is provisioned. Data-plane storage
(ADLS Gen2) stays put — external-table data is never moved.

### How data moves

Managed-table bytes move via **Delta Sharing + `DEEP CLONE`** — the tool
creates an internal share on source, the target consumes it, and each managed
table is cloned into the target's UC. No intermediate object storage is
required. External tables, views, functions, and governance objects are
replayed via DDL.

### Design tenets

- **Idempotent** — every worker is re-runnable; status is tracked per-object
  in a Delta table, so re-runs only act on pending objects.
- **No source mutation** — the source workspace is never modified. Tables
  protected by a row filter, column mask, or ABAC policy are **not migrated**
  (see the policy-protected-tables note below); they are excluded and reported
  in the dashboard for manual handling.
- **Standalone jobs** — UC, Hive, and Governance are independent workflows;
  operators decide ordering.
- **Serverless-only compute** — no cluster management, no init scripts.
- **Auditable** — every action lands a row in `migration_status`, with a
  Lakeview dashboard summarising counts, failures, and durations.

---

## 2. Migration phases

A full control-plane migration follows a phased approach. This tool automates
**Phase 2** (and contributes to Phase 3/4); the other phases are operator- and
methodology-driven.

| Phase | Key actions | Tool involvement |
|---|---|---|
| **0. Discovery** | Inventory assets per workspace; classify complexity; identify integrations; size managed-table data volume. | `discovery` job inventories UC + Hive objects. |
| **1a. Infrastructure / 1b. Identity** | Create the target workspace + UC metastore in the Account Console. Networking, Private Link, identity (SSO/SCIM), CMK. | Out of scope — operator/IaC. |
| **2. UC & data migration** | External tables (fast), managed tables via DEEP CLONE (slow), views, functions, grants, governance. Validate integrity. | **Core of this tool** — `migrate_uc`, `migrate_hive`, `migrate_governance`. |
| **3. Workspace assets** | Notebooks, jobs, policies, warehouses, dashboards, secrets. | **Terraform exporter** (Section 8). |
| **4. Stateful services** | Lakeflow Connect, DLT, Model Serving, Apps, MLflow. (Vector Search + Online Tables out of scope — migrate out of band.) | Optional `migrate_lfc` job + Terraform exporter (DLT/Model Serving/Apps) — Sections 7–8. |
| **5. Validation & cutover** | Verify counts/rows/grants; update DNS/SSO/SCIM/integrations; enable new, disable old. | Section 9. |
| **6. Decommission** | Dual-run grace period; archive audit logs; delete old workspace + Private Link. | Section 10. |

---

## 3. What it covers — approach by data type

For each object class: the **complexity**, the **migration approach**, and
**how this tool handles it**. Objects are sequenced by the priority in
Section 3.7.

The Hive path migrates `hive_metastore` content **like-for-like** into the
target workspace's own `hive_metastore` — same database/table names, same
storage — rather than upgrading it into a UC catalog.

### 3.1 Hive managed tables — DBFS-root storage
- **Complexity:** HIGH. Data lives in the workspace-scoped DBFS root; the new
  workspace has its own DBFS storage it can't reach, so bytes must be copied.
- **Approach:** two-hop staging copy — write the source table to a shared
  staging path, then re-create it as a MANAGED table in the target's own DBFS
  root.
- **This tool:** `migrate_hive` with `migrate_hive_dbfs_root: true` stages the
  DBFS-root bytes via `hive_dbfs_staging_path` (a shared `abfss://` location both
  workspaces can reach) and re-creates the table **managed** in the target
  `hive_metastore` (same database/table name) — DBFS root must be enabled on the
  target.

### 3.2 Hive managed tables — non-DBFS storage
- **Complexity:** MEDIUM–HIGH. Data is on customer-owned storage but registered
  as *managed* in Hive.
- **Approach:** re-register against the same location (Delta reads schema from
  the log; non-Delta needs `MSCK REPAIR`).
- **This tool:** `migrate_hive` (`hive_managed_nondbfs_worker`) re-registers the
  table in the target `hive_metastore` pointing at the same storage (LOCATION
  preserved).

### 3.3 Hive external tables — external storage (ADLS)
- **Complexity:** LOW. Data stays in ADLS; only metadata (DDL) is recreated.
- **Approach:** recreate the table pointing at the same location; like-for-like
  **Hive external → Hive external** (same storage path).
- **This tool:** `migrate_hive` (`hive_external_worker`) replays the DDL into the
  target `hive_metastore` against the same ADLS path (target FQN == source FQN).

### 3.4 External Hive metastore
- **Complexity:** LOW. The metastore lives in customer-configured storage
  (MySQL / Azure SQL); connect the new workspace to the same metastore and all
  metadata is retained.
- **Approach:** set up credentials + init scripts on target, verify
  connectivity.
- **This tool:** out of scope — operator reconnects the external metastore. See
  [external_hive_metastore.md](reference/external_hive_metastore.md).

### SPN permissions on hive_metastore (like-for-like Hive path)

The Hive path migrates `hive_metastore` content **like-for-like** into the
target workspace's own `hive_metastore` (same database/table names, same
storage). The SPN needs:

**Source workspace (read):**
- Legacy Hive `SELECT` + `READ_METADATA` on the migrated `hive_metastore`
  databases/tables (for `SHOW CREATE TABLE`, `SHOW GRANTS`, and reading rows for
  the DBFS-root staging copy).
- Source DBFS-root read access (runs on a classic cluster; workspace-level).
- ADLS storage account key (secret) for ADLS-backed HMS external/non-DBFS tables
  (legacy `fs.azure.account.key`; UC vending doesn't cover HMS `LOCATION`s).
- **Write** access to the shared staging container (`hive_dbfs_staging_path`).

**Target workspace (write):**
- Legacy Hive `CREATE` on `hive_metastore` (create databases) and on each target
  database (create tables).
- Target **DBFS root enabled** + write access.
- **Read** on the shared staging container.
- Required `/mnt` mounts pre-existing (recreate them first — the tool never
  touches mount credentials; pre_check verifies each required mount exists).

#### Target SQL Warehouse — storage access for `hive_metastore` writes

The Hive workers issue the target-side `CREATE DATABASE` / `CREATE TABLE`
through the **target SQL warehouse**. Legacy `hive_metastore` tables bypass
Unity Catalog and access storage directly through the Hadoop filesystem, so the
target warehouse must be able to reach the relevant cloud storage **itself** —
UC external locations do NOT cover `hive_metastore` `LOCATION`s. Configure this
in **Admin Settings → SQL Warehouse Settings → Data Access Configuration** (a
workspace-level setting that applies to all SQL warehouses):

- **Shared staging container** (`hive_dbfs_staging_path`) — the DBFS-root
  two-hop reads staged Delta from here via the target warehouse.
- **External-table ADLS/S3/GCS paths** — any `hive_metastore` **external** table
  is recreated with its original `LOCATION`, so the warehouse must authenticate
  to that path.

**The auth mechanism is the customer's choice** — the tool does not require a
storage account key. Any of these in the warehouse Data Access Configuration
works, per your security posture:
- **Azure:** OAuth with a **service principal + client secret**
  (`fs.azure.account.auth.type…=OAuth`, `…oauth.provider.type`, `…client.id`,
  `…client.secret`, `…client.endpoint`), **or** a storage **account key**
  (`spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net`).
- **AWS/GCP:** the equivalent instance-profile / service-account or key-based
  Hadoop configs.

Without this, external-ADLS tables (and the DBFS-root staging read) fail on the
target with `Invalid configuration value detected for fs.azure.account.key` (or
the cloud's equivalent). This is a one-time workspace setup step, independent of
the migration SPN's UC grants above.

**What changed vs the UC-upgrade path — no longer needs:**
- `CREATE CATALOG` on the metastore.
- UC `CREATE SCHEMA` / `CREATE TABLE` / `USE CATALOG` in a UC catalog.
- Delta Sharing privileges (`CREATE SHARE`, `CREATE RECIPIENT`) for the Hive path.
- UC external-location grants (`CREATE EXTERNAL TABLE` / `READ FILES`) for the
  Hive path.

**Now needs:** legacy Hive `CREATE` on target `hive_metastore` + databases,
target DBFS root enabled + write access, and the **target SQL warehouse's Data
Access Configuration** set up with storage access (SP/OAuth or account key —
customer's choice) to the shared-staging container and any external-table
`LOCATION`s (see the subsection above).

### 3.5 UC managed tables
- **Complexity:** MEDIUM–HIGH. Data is in UC-managed storage; the new metastore
  has different managed storage, so bytes must be copied.
- **Approach:** Delta Sharing between metastores → `DEEP CLONE` (Delta) / CTAS
  (non-Delta) into the new metastore.
- **This tool:** `migrate_uc` (`managed_table_worker`) does exactly this via the
  internal `cp_migration_share`. Managed **Iceberg** tables are skipped by
  default; opt into DDL-replay + re-ingest with `iceberg_strategy: ddl_replay`
  (loses snapshot history / time-travel).

### 3.6 UC external tables
- **Complexity:** LOW. Data stays in ADLS; only metadata migrates.
- **Approach:** create storage credentials + external locations on the new
  metastore pointing at the same ADLS paths; recreate `CREATE TABLE … LOCATION`.
- **This tool:** `migrate_uc` (`external_table_worker`) replays the DDL against
  the same location. (Storage credentials + external locations are an operator
  prerequisite — see Setup.)

### 3.7 UC volumes
- **Complexity:** LOW (external) / MEDIUM (managed).
- **Approach:** external → recreate the external location + volume definition;
  managed → copy files to the new managed storage, then recreate.
- **This tool:** `migrate_uc` (`volume_worker`) — external via metadata replay,
  managed with file-level copy.

### 3.8 Views, functions
- **Complexity:** LOW.
- **Approach:** export definitions; recreate in dependency order (functions
  before the views / filters / masks that reference them).
- **This tool:** `migrate_uc` (`views_worker`, `functions_worker`) for UC;
  `migrate_hive` (`hive_views_worker`, `hive_functions_worker`) for Hive.

### 3.9 Governance, sharing, and other UC objects
Migrated by `migrate_governance` (fine-grained) and `migrate_uc` (grants,
models):

| Object | How this tool handles it |
|---|---|
| Grants (all levels) | `grants_worker` / `hive_grants_worker` — applied after objects exist |
| Tags, ABAC policies | `tags_worker`, `policies_worker` |
| Row filters, column masks | `row_filters_worker`, `column_masks_worker` (DDL replay; see RLS/CM note in README) |
| Comments, table properties | preserved by `DEEP CLONE` for Delta; DDL replay otherwise |
| Registered models | `models_worker` — shell + each version (MLflow **download from source → re-register on target**, which ingests the artifacts into target-managed storage) + aliases. Requires `mlflow` on the serverless runtime. |
| Connections, foreign catalogs | `connections_worker`, `foreign_catalogs_worker` (connection secrets are **not** exported — re-enter) |
| Customer shares, recipients, providers | `sharing_worker` (SPN must own or hold `USE SHARE`/`USE RECIPIENT` — see README) |
| Lakehouse monitors | `monitors_worker` (metric history does not transfer) |

### 3.10 Data-migration priority & sequencing
Recommended order (fast/unblocking first, heavy/dependent last):

| # | Category | Reason |
|---|---|---|
| 1 | UC external tables | No data copy; unblocks dependent views/jobs |
| 2 | Hive external tables | No data copy; upgrade to UC |
| 3 | UC views, functions | Depend on tables |
| 4 | Hive views | Depend on tables + namespace rewrite |
| 5 | UC managed tables | Data copy; start largest early |
| 6 | Hive managed (non-DBFS) | Re-registration |
| 7 | Hive managed (DBFS-root) | Most complex; bytes copied out first |
| 8 | Volumes | After tables |
| 9 | Streaming tables / MVs | Last — see §4 (out of scope for this tool) |

---

## 4. What is out of scope (and where it's handled)

### Hard-skipped by this tool
`migrate_uc` runs the `mv_st_worker`, which **skips all Materialized Views and
Streaming Tables** with status `skipped_by_stateful_service_migration` — their
stream/rebuild state cannot be replayed by DDL. Their schema and grants still
migrate, but the objects are not rebuilt by this tool. Rebuild them on target
via a pipeline refresh at cutover.

### Handled via the Terraform exporter (Section 8)
These are **not** migrated by this tool; use the exporter + the guidance in
Section 8:
- **DLT / Lakeflow Declarative Pipelines** — exporter recreates pipeline config
  + notebooks; full refresh on target.
- **Model Serving endpoints** — exporter recreates endpoint config; this tool's
  `models_worker` supplies the served model; blue-green cutover.
- **Apps** — exporter emits the app definition only; source + redeploy are
  separate.

### Available now via optional tool jobs (Section 7)
- **Lakeflow Connect** (`migrate_lfc`).

### Not covered (out-of-band)
- **Vector Search** and **Online Tables** — the `migrate_vector_search` /
  `migrate_online_tables` jobs still deploy with the bundle but are **out of
  scope / unsupported**; this guide does not cover them. Recreate Vector Search
  indexes (re-embed from the migrated source table) and Online/synced tables
  directly on target.
- **Lakebase** (`pg_dump`/`pg_restore`), **Online Feature Store**, **Genie
  spaces**, **Agent Bricks**, **MLflow experiments / workspace-registry models**
  (`mlflow-export-import`).

---

## 5. Prerequisites

### Tooling (operator's machine)
- **Databricks CLI** ≥ 0.220.
- **Terraform 1.5.7+** locally — the CLI's bundled Terraform has an expired GPG
  signing key; point the CLI at your local Terraform (Setup Step 6).
- **uv** / `pip` — only for running the unit tests.

### Access
- **Both workspaces** on the same cloud; same metastore region recommended for
  Delta Sharing performance (cross-region works but volume propagation may lag).
- **Service Principal (SPN)** — OAuth M2M, workspace **admin on both** source
  and target; runs all migration jobs.
- **Network connectivity** — serverless compute reachable from the target; if
  source uses Private Link, the SPN's source-side REST calls must traverse it.

### Workspaces
- **Target must be empty or non-overlapping** — `pre_check` surfaces collisions.
- **Delta Sharing enabled** on both metastores.
- **Target storage credentials + external locations** created for any external
  table / DBFS-root path the migration touches.
- **`mlflow` on the serverless job runtime** if you migrate registered models —
  both discovery (alias enumeration) and `models_worker` (download-then-register)
  use it. The serverless default env includes it; if a run reports
  `ModuleNotFoundError: mlflow`, add `%pip install mlflow` or pin it in the job's
  environment spec.

### Policy-protected tables are excluded (RLS / column mask / ABAC)
Tables protected by a **row filter**, **column mask**, or an **ABAC policy**
are **not migrated**. Copying them risks silent data loss — the copy reads
*through* the policy, so it captures filtered/masked data — and there is no
safe way to read the raw data without altering the live source. So discovery:
- **excludes** every affected table from migration (recorded with status
  `skipped_policy_protected` and the reason), and
- **surfaces** them in the migration **dashboard** ("Policy-protected tables —
  manual action required") so you can migrate them deliberately (e.g. remove
  the policy on source, migrate, re-apply on target — a decision you own).

No source-state preparation is needed; nothing on the source is changed.

---

## 6. Setup

### Step 1 — Clone the repo
```bash
git clone git@github.com:databricks-solutions/workspace-migration.git
cd workspace-migration
```

### Step 2 — Create the migration SPN
Create an OAuth service principal in the account console, add it as **admin on
both workspaces**, and generate a client secret:
```bash
databricks account workspace-assignments update <source-workspace-id> \
  --principal-id <spn-id> --permissions ADMIN
databricks account workspace-assignments update <target-workspace-id> \
  --principal-id <spn-id> --permissions ADMIN
databricks account service-principal-secrets create <spn-id>   # shown once
```

### Step 3 — Store the SPN secret (source workspace)
The bundle's notebooks execute on the workspace they are deployed to (Step 6),
which is the **migration source**. They read the SPN secret with
`dbutils.secrets.get(...)` against that *local* workspace, so the scope and
secret must live on the **source** workspace — not the target.
```bash
databricks secrets create-scope migration --profile source-workspace
databricks secrets put-secret migration spn-secret --profile source-workspace
```

### Step 4 — Grant SPN privileges
The SPN must be able to **see and read every source object** (discovery runs
as the SPN) and **create objects on the target**. Two options:

**Option A — least privilege (recommended).** UC has no `ON ALL CATALOGS`
securable, and privileges granted **on a catalog inherit** to all current and
future schemas/tables/volumes/functions/models. So grant per-catalog (loop
over `SHOW CATALOGS`, skipping `system`/`samples`/tool-owned) — this also
covers **foreign catalogs** (a foreign catalog is a catalog):
```sql
-- SOURCE (run as metastore admin), once per catalog <cat>:
GRANT USE CATALOG    ON CATALOG `<cat>` TO `<spn-application-id>`;
GRANT USE SCHEMA     ON CATALOG `<cat>` TO `<spn-application-id>`;
GRANT SELECT         ON CATALOG `<cat>` TO `<spn-application-id>`;
GRANT READ VOLUME    ON CATALOG `<cat>` TO `<spn-application-id>`;
GRANT EXECUTE        ON CATALOG `<cat>` TO `<spn-application-id>`;
-- SOURCE, once per CONNECTION <conn> (no bulk/metastore-level grant exists;
-- without this the SPN cannot see the connection and discovery silently
-- skips it and its foreign catalog):
GRANT USE CONNECTION ON CONNECTION `<conn>` TO `<spn-application-id>`;
-- SOURCE, Delta Sharing family (metastore-wide grants ARE valid here):
GRANT USE PROVIDER ON METASTORE   TO `<spn-application-id>`;
ALTER SHARE     `<share_name>`     OWNER TO `<spn-application-id>`;
ALTER RECIPIENT `<recipient_name>` OWNER TO `<spn-application-id>`;
-- SOURCE, ONLY when migrate_hive_dbfs_root=true — the DBFS-root two-hop stages
-- data through hive_dbfs_staging_path; the SPN's write probe (check_hive_dbfs_root_config)
-- and STAGE-1 copy need file access on the external location backing that path.
-- Grant these on BOTH workspaces' external location for the shared staging path:
GRANT READ FILES  ON EXTERNAL LOCATION `<staging-ext-loc>` TO `<spn-application-id>`;
GRANT WRITE FILES ON EXTERNAL LOCATION `<staging-ext-loc>` TO `<spn-application-id>`;
```
```sql
-- TARGET
GRANT CREATE CATALOG         ON METASTORE TO `<spn-application-id>`;
GRANT CREATE CONNECTION      ON METASTORE TO `<spn-application-id>`;  -- recreate connections
GRANT USE PROVIDER           ON METASTORE TO `<spn-application-id>`;
GRANT CREATE EXTERNAL VOLUME ON EXTERNAL LOCATION `<ext-loc>` TO `<spn-application-id>`;  -- external volumes
```

**Option B — metastore admin (easy setup).** Add the SPN to the group set as
the metastore admin. It then sees/creates every object type with no
per-catalog/per-connection grants. Broader privilege (may be unacceptable to
security-sensitive customers); data reads (`SELECT`/`READ VOLUME`) still apply,
so pair it with the data grants above if the SPN isn't already granted them.

> Do **not** use `GRANT … ON ALL CATALOGS` — it is not valid UC syntax
> (`PARSE_SYNTAX_ERROR`). Use the per-catalog grants above (they inherit
> downward), or Option B.

### Step 5 — Configure `config.yaml`
`config.yaml` is git-ignored; copy the example and fill in real values locally:
```bash
cp config.example.yaml config.yaml
```
Key fields (full reference in the [README](../README.md#config-reference-configyaml)):
```yaml
source_workspace_url: "https://adb-<source-id>.<n>.azuredatabricks.net"
target_workspace_url: "https://adb-<target-id>.<n>.azuredatabricks.net"
spn_client_id:        "<spn-application-id>"
spn_secret_scope:     "migration"
spn_secret_key:       "spn-secret"
catalog_filter:  []                 # scope the run
rls_cm_strategy: ""                 # DEPRECATED — RLS/CM/ABAC tables are excluded + reported
iceberg_strategy: "ddl_replay"      # if managed Iceberg present
migrate_hive_dbfs_root: false       # true + hive_dbfs_staging_path for DBFS-root Hive (two-hop staging)
```
> Don't edit the workspace-side copy — `bundle deploy` overwrites it from your
> local copy.

### Step 6 — Deploy the bundle (source workspace)
Deploy to the **source** workspace. The jobs run there and use local Spark to
inventory the source (discovery) and the SPN secret (Step 3) to reach the
target for writes. Deploying to the target instead makes discovery scan the
empty target *as if it were the source* — a silent no-op migration.
```bash
export DATABRICKS_TF_VERSION=1.5.7
export DATABRICKS_TF_EXEC_PATH=$(which terraform)
databricks bundle deploy -t dev \
  --var migration_spn_id=<spn-application-id> \
  --profile source-workspace
```
This creates the workflows — `pre_check`, `discovery`, `migrate_uc`,
`migrate_hive`, `migrate_governance`, `migrate_lfc` (plus integration tests) —
the Lakeview dashboard, and the workspace `config.yaml`. (`migrate_vector_search`
and `migrate_online_tables` also deploy but are out of scope / unsupported — see
§4.)

---

## 7. Running the tool

Recommended order: `pre_check` → `discovery` → `pre_check` (again, for
collisions) → `migrate_uc` → `migrate_hive` (if applicable) →
`migrate_governance` → then the optional `migrate_lfc` job.

### Step 1 — `pre_check`
Validates connectivity, SPN grants, and (after discovery) target collisions.
```sql
SELECT check_name, status, severity, error_message
FROM migration_tracking.cp_migration.pre_check_results
WHERE run_id = <latest_run_id> ORDER BY severity DESC;
```

### Step 2 — `discovery`
Scans source via Delta Sharing + REST; writes `discovery_inventory` (one row
per source object). Also prints DLT-managed-table and RLS/CM warnings.

### Step 3 — `pre_check` again (collision pass)
With `on_target_collision: fail` (default) an unexpected target object blocks
`migrate_uc`; rename/drop it, or set `on_target_collision: skip`.

### Step 4 — `migrate_uc`
UC data plane: managed/external tables, views, functions, volumes, models, plus
UC grants (`setup_sharing` → `orchestrator` → workers → `cleanup_staging` →
`summary_uc`). MV/ST rows are skipped here (§4). Re-runs are safe.
```sql
SELECT object_type, status, COUNT(*) n
FROM migration_tracking.cp_migration.migration_status
WHERE source_type='uc' GROUP BY object_type, status;
```

### Step 5 — `migrate_hive` (only if migrating from Hive)
Same shape, scoped to Hive sources; writes **like-for-like** into the target
workspace's own `hive_metastore` (same database/table names — no UC catalog).
See [external_hive_metastore.md](reference/external_hive_metastore.md)
for the classic-compute cluster/init-script requirements for ADLS-backed Hive
tables.

### Step 6 — `migrate_governance` (run last of the core three)
Assumes target tables/views/volumes already exist. Replays tags, RLS, column
masks, comments, monitors, customer shares, foreign catalogs, connections,
policies. **Do not run against an empty target.**

> **Vector Search and Online Tables are out of scope / unsupported.** The
> `migrate_vector_search` and `migrate_online_tables` jobs deploy with the
> bundle but are not part of the supported path and are not documented here
> (see §4). Recreate Vector Search indexes and Online/synced tables out of band.

### Step 7 — `migrate_lfc` (optional)
Migrates Lakeflow Connect ingestion pipelines (cross-workspace = cut-over, not
in-place). **Tier 1** (query-based DB + SaaS row_filter): clone history →
recreate with a `row_filter` cursor boundary → unified view. **Tier 2** (CDC /
gateway): recreate gateway + ingestion pipeline, create-only; customer starts
the re-hydrate at cutover. SaaS cursor is a mandatory operator input
(`lfc_saas_cursor_columns`); the target UC connection must be pre-created
(`lfc_target_connection_name`). Full detail: `migration_status` rows
`lfc_table` / `lfc_pipeline` / `lfc_gateway` / `lfc_view`.

### Step 8 — Verify
```sql
SELECT object_type, status, COUNT(*) n
FROM migration_tracking.cp_migration.migration_status
GROUP BY object_type, status ORDER BY object_type;
-- Pending (discovered but no status row):
SELECT d.object_type, COUNT(*) pending
FROM migration_tracking.cp_migration.discovery_inventory d
LEFT JOIN migration_tracking.cp_migration.migration_status m
  ON m.object_name=d.object_name AND m.object_type=d.object_type
WHERE m.object_name IS NULL GROUP BY d.object_type;
```

---

## 8. Migrating non-UC assets via the Terraform exporter

Workspace assets and several stateful services are migrated with the
**Databricks Terraform exporter** (`terraform-provider-databricks exporter`),
not this tool. Run one export with the asset listings, review the HCL, point
the provider at the target, and `terraform apply`.

```bash
export DATABRICKS_CONFIG_PROFILE=<source-profile>
terraform-provider-databricks exporter -skip-interactive \
  -directory=./export -listing=notebooks,wsfiles,repos,jobs,compute,dlt,model-serving,apps,dashboards,queries,secrets,policies,pools,sql-endpoints
```

### 8.1 Workspace assets (Phase 3)
Notebooks/files, Git repos, jobs (excl. DABs jobs), cluster policies, instance
pools, SQL warehouses, queries/alerts, dashboards, secret scopes (values
redacted — use `-export-secrets` or re-provision), IP access lists, global init
scripts, identities. IDs (warehouse/policy/pool) change; references are
auto-updated in the exported HCL.

### 8.2 DLT / Lakeflow Declarative Pipelines
The exporter migrates DLT pipelines well (validated end-to-end). See
[dlt_pipeline_migration.md](reference/dlt_pipeline_migration.md) for the full guide.
- **Exports** the full pipeline config + wires in its notebooks; the migrated
  pipeline **runs correctly** on target.
- **Use the full asset listing** (`dlt,notebooks,wsfiles,repos`), not `dlt`
  alone — `-listing=dlt` misses transitive Python imports / non-library files.
- **Pre-create the UC catalog/schema** (this tool) — the exporter doesn't.
- **No state migrates** → full refresh at cutover, in dependency order (the
  exporter does not capture inter-pipeline ordering — derive it from UC lineage
  or supply it manually).

### 8.3 Model Serving endpoints
See [model_serving_and_apps_migration.md](reference/model_serving_and_apps_migration.md).
- The exporter captures the **full endpoint config** (served entities, traffic,
  scale-to-zero) — **only when the endpoint is `READY`** (a provisioning
  endpoint exports as a name-only stub) — and wires it to the model.
- The served model itself comes from this tool's **`models_worker`** (metadata +
  version artifacts). Align the endpoint's `entity_version` with the migrated
  version. **Foundation-model endpoints are auto-skipped** (they exist on every
  workspace). Blue-green cutover; endpoint URLs change.

### 8.4 Apps
- The exporter emits **only the app definition** (name, and `resources`/
  `description` if set) — **no source code, no deployment**. Applying it yields a
  non-functional shell.
- Full app migration = recreate the definition (**remap `resources` to target
  IDs**) + migrate the source (workspace files) + `databricks apps deploy
  --source-code-path /Workspace/<path>`.

### 8.5 Exporter gotchas
- **Export serving endpoints only when `READY`** (else name-only stub).
- **Registering a UC model needs a UC-enabled cluster** (`data_security_mode =
  SINGLE_USER`); a No-Isolation cluster fails with `PERMISSION_DENIED … clusters
  that don't have Unity Catalog enabled`.
- Serverless job base env lacks `mlflow` → `%pip install mlflow` (or use an ML
  runtime on classic compute).
- App `--source-code-path` must be a `/Workspace/...` path.

---

## 9. Validation & cutover

### Data validation checklist
After each category, verify on target:
- Table counts and row counts match (sample very large tables).
- Schema (names, types, nullability) and partition structure preserved.
- Table properties and comments preserved.
- Grants/permissions replayed and validated with test users.
- Views return expected results; functions execute; downstream jobs/queries run.

### External integrations cutover
Update every system referencing the old workspace: CI/CD (URLs, tokens),
Terraform (provider host, workspace ID), orchestrators (Airflow/ADF), BI tools
(JDBC/ODBC, warehouse IDs), SCIM/Entra (endpoints, SSO), API clients,
monitoring, and Kafka/Event Hubs endpoints.

### Non-recoverable assets (plan around these)
Job/query run history, notebook revision history, PATs/OAuth tokens, Git
credentials, DBFS-root data (AzCopy separately), UC connection passwords, and
storage-credential keys are **not** transferable — re-provision on target.

---

## 10. Decommission

- **Dual-run grace period** (2–4 weeks recommended).
- Archive audit logs from the old workspace (`system.access.audit` is tied to
  the old metastore).
- Confirm all integrations point to the new workspace.
- Delete the old workspace + Private Link endpoints; clean up the old managed
  resource group.

---

## 11. Risk matrix

| Risk | Impact | Mitigation |
|---|---|---|
| Managed-table `DEEP CLONE` too slow | Extends window | Start largest tables first; parallel clones; incremental sync for huge tables |
| Secret values not re-provisioned | Jobs/pipelines fail | Document scopes; prepare values before cutover |
| Streaming gap during cutover | Data loss/dup | Precise timing; idempotent writes; dual-write period |
| External integrations missed | Production failures | Comprehensive discovery; customer checklist (§9) |
| Old workspace ID hardcoded | Runtime failures | Search repos for the old ID; find-and-replace |
| Serving endpoint URL change | Inference downtime | Blue-green; update clients before decommission |
| Permission/grant gaps | Access denied | Comprehensive GRANT replay; validate with test users |

---

## 12. Troubleshooting & FAQ

- **`bundle deploy` fails "key expired"** — use a local Terraform (Setup Step 6).
- **`pre_check` reports collisions you didn't create** — rename/drop them, or set
  `on_target_collision: skip`.
- **Customer share not appearing in discovery** — the SPN must own it or hold
  `USE SHARE` (see README, Delta Sharing prerequisites).
- **RLS/CM/ABAC table not migrated** — this is by design: policy-protected
  tables are excluded and listed in the dashboard's "Policy-protected tables"
  panel. Migrate them manually (remove policy on source → migrate → re-apply).
- **Volume contents missing** — cross-region Delta Sharing propagation lags;
  re-run `migrate_uc` (idempotent).
- **A worker keeps failing** — check `migration_status.error_message` + run logs;
  re-run (only failed objects retry).

---

## 13. Going deeper

- Architecture + per-job task graph: [peer-review-diagrams.md](reference/peer-review-diagrams.md)
- Workflow split rationale: [workflow_split_design.md](reference/workflow_split_design.md)
- Idempotency model: [idempotency_audit.md](reference/idempotency_audit.md)
- Retry / resumability: [retry_resumability.md](reference/retry_resumability.md)
- Stateful services scope: [stateful_services_phase.md](reference/stateful_services_phase.md)
- DLT migration (Terraform exporter): [dlt_pipeline_migration.md](reference/dlt_pipeline_migration.md)
- Model Serving & Apps migration: [model_serving_and_apps_migration.md](reference/model_serving_and_apps_migration.md)
- External Hive Metastore: [external_hive_metastore.md](reference/external_hive_metastore.md)
