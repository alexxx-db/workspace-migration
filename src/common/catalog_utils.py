from __future__ import annotations

import contextlib
import logging
import re
from collections import defaultdict, deque

from databricks.sdk.errors import PermissionDenied

from common.auth import AuthManager

logger = logging.getLogger("catalog_utils")


def _sql_in_literal(values: set[str]) -> str:
    """Render a set of strings as a comma-separated SQL string literal list."""
    if not values:
        return "''"
    escaped = [v.replace("'", "''") for v in values]
    return ", ".join(f"'{v}'" for v in escaped)


# Matches both quoted (`cat`.`schema`.`name`) and unquoted (cat.schema.name)
# three-part references. Names can contain letters, digits, underscores.
_QUOTED_REF = re.compile(r"`([^`]+)`\s*\.\s*`([^`]+)`\s*\.\s*`([^`]+)`")
_UNQUOTED_REF = re.compile(r"\b([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\b")


def _extract_three_part_refs(sql: str, view_set: set[str]) -> set[str]:
    """Extract three-part FQN references from a view body, filtering to
    those that appear in ``view_set``. Returns FQNs in backtick form
    (`cat`.`schema`.`name`) to match ``views_worker`` input keys.
    """
    refs: set[str] = set()
    for m in _QUOTED_REF.finditer(sql):
        fqn = f"`{m.group(1)}`.`{m.group(2)}`.`{m.group(3)}`"
        if fqn in view_set:
            refs.add(fqn)
    for m in _UNQUOTED_REF.finditer(sql):
        fqn = f"`{m.group(1)}`.`{m.group(2)}`.`{m.group(3)}`"
        if fqn in view_set:
            refs.add(fqn)
    return refs


# Small alias for per-table try/except blocks in governance discovery —
# absence of a given tag table / REST endpoint shouldn't abort discovery.
_suppress = lambda: contextlib.suppress(Exception)  # noqa: E731

SYSTEM_CATALOGS = frozenset({"system", "hive_metastore", "__databricks_internal", "samples"})
EXCLUDED_SCHEMAS = frozenset({"default", "information_schema"})

HIVE_CATALOG = "hive_metastore"

# Categories returned by CatalogExplorer.categorize_hive_table — drive which worker
# handles each table in Phase 2.
HIVE_CATEGORIES = frozenset(
    {
        "hive_view",
        "hive_external",
        "hive_managed_dbfs_root",
        "hive_managed_nondbfs",
    }
)

_TABLE_TYPE_MAP = {
    "MANAGED": "managed_table",
    "EXTERNAL": "external_table",
    "VIEW": "view",
    "MATERIALIZED_VIEW": "mv",
    "STREAMING_TABLE": "st",
}


class CatalogExplorer:
    """Utility for discovering and classifying Unity Catalog objects."""

    def __init__(self, spark: object, auth_manager: AuthManager) -> None:
        self.spark = spark
        self.auth_manager = auth_manager
        # Per-instance memoization for list_foreign_catalogs(): both
        # discovery.py and list_foreign_catalog_names() invoke it in the
        # same run, and the underlying ``catalogs.list(include_browse=True)``
        # is slow at scale.
        self._foreign_catalogs_cache: list[dict] | None = None

    # ------------------------------------------------------------------
    # Catalogs & schemas
    # ------------------------------------------------------------------

    def list_catalogs(self, filter_list: list[str] | None = None) -> list[str]:
        """Return non-system catalogs, optionally filtered by *filter_list*."""
        rows = self.spark.sql("SHOW CATALOGS").collect()  # type: ignore[attr-defined]
        catalogs = [row.catalog for row in rows if row.catalog not in SYSTEM_CATALOGS]
        if filter_list:
            allowed = set(filter_list)
            catalogs = [c for c in catalogs if c in allowed]
        return catalogs

    def list_schemas(self, catalog: str) -> list[str]:
        """Return non-default schemas within *catalog*."""
        rows = self.spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()  # type: ignore[attr-defined]
        return [row.databaseName for row in rows if row.databaseName not in EXCLUDED_SCHEMAS]

    # ------------------------------------------------------------------
    # Tables / views
    # ------------------------------------------------------------------

    def classify_tables(self, catalog: str, schema: str) -> list[dict]:
        """Query information_schema.tables and classify each object."""
        query = (
            f"SELECT table_name, table_type, data_source_format "
            f"FROM `{catalog}`.`information_schema`.`tables` "
            f"WHERE table_schema = '{schema}'"
        )
        rows = self.spark.sql(query).collect()  # type: ignore[attr-defined]
        results: list[dict] = []
        for row in rows:
            object_type = _TABLE_TYPE_MAP.get(row.table_type, row.table_type)
            results.append(
                {
                    "fqn": f"`{catalog}`.`{schema}`.`{row.table_name}`",
                    "object_type": object_type,
                    "table_type": row.table_type,
                    "data_source_format": row.data_source_format,
                }
            )
        return results

    def detect_dlt_managed(self, table_fqn: str) -> tuple[bool, str | None]:
        """Check whether a table is managed by a DLT pipeline. Safe to call on views (returns False)."""
        try:
            row = self.spark.sql(f"DESCRIBE DETAIL {table_fqn}").first()  # type: ignore[attr-defined]
        except Exception:
            return (False, None)
        properties: dict = row.properties if row.properties else {}
        pipeline_id = properties.get("pipelines.pipelineId")
        return (pipeline_id is not None, pipeline_id)

    def get_table_row_count(self, table_fqn: str) -> int:
        """Return the row count of a table."""
        row = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_fqn}").first()  # type: ignore[attr-defined]
        return row.cnt  # type: ignore[union-attr]

    def get_table_size_bytes(self, table_fqn: str) -> int:
        """Return the sizeInBytes from DESCRIBE DETAIL."""
        row = self.spark.sql(f"DESCRIBE DETAIL {table_fqn}").first()  # type: ignore[attr-defined]
        return row.sizeInBytes  # type: ignore[union-attr]

    def get_table_format(self, table_fqn: str) -> str | None:
        """Return the table format from DESCRIBE DETAIL (e.g. 'delta', 'iceberg').

        Returns None on any error — callers treat a missing format as delta
        for backward compatibility with pre-Iceberg-support tools.
        """
        try:
            row = self.spark.sql(f"DESCRIBE DETAIL {table_fqn}").first()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            return None
        fmt = getattr(row, "format", None)
        return fmt.lower() if isinstance(fmt, str) else None

    # ------------------------------------------------------------------
    # DDL / create statements
    # ------------------------------------------------------------------

    def get_create_statement(self, object_fqn: str) -> str:
        """Return a CREATE OR REPLACE VIEW/TABLE statement for the given object.

        For views, prefer information_schema.views (view_definition) so we get
        the original SQL body without any SHOW CREATE quirks. For UC-managed
        Iceberg tables (which don't support SHOW CREATE TABLE), synthesize a
        CREATE TABLE statement from information_schema.columns.
        """
        parts = object_fqn.strip("`").split("`.`")
        if len(parts) == 3:
            catalog, schema, name = parts
            try:
                row = self.spark.sql(  # type: ignore[attr-defined]
                    f"""
                    SELECT view_definition
                    FROM `{catalog}`.`information_schema`.`views`
                    WHERE table_schema = '{schema}' AND table_name = '{name}'
                    LIMIT 1
                    """
                ).first()
                if row is not None and row.view_definition:
                    return f"CREATE OR REPLACE VIEW `{catalog}`.`{schema}`.`{name}` AS {row.view_definition}"
            except Exception:  # noqa: BLE001
                pass  # fall through to SHOW CREATE
        try:
            row = self.spark.sql(f"SHOW CREATE TABLE {object_fqn}").first()  # type: ignore[attr-defined]
            return row.createtab_stmt  # type: ignore[union-attr]
        except Exception as exc:  # noqa: BLE001
            # UC-managed Iceberg raises MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED for
            # SHOW CREATE TABLE. Fall back to synthesizing the DDL from
            # information_schema.columns so iceberg migration can proceed.
            if ("MANAGED_ICEBERG" in str(exc) or "SHOW CREATE TABLE" in str(exc)) and len(parts) == 3:
                synthetic = self._build_create_stmt_from_columns(parts[0], parts[1], parts[2])
                if synthetic:
                    return synthetic
            raise

    @staticmethod
    def strip_filter_mask_clauses(ddl: str) -> str:
        """Remove row filter / column mask clauses from a CREATE TABLE DDL.

        SHOW CREATE TABLE on a UC table with legacy RLS/CM applied includes
        inline ``WITH ROW FILTER ... ON (...)`` and per-column ``MASK
        <fqn> [USING (cols)]`` clauses. Replaying that DDL on target fails
        with ``ROUTINE_NOT_FOUND`` because the filter/mask functions
        haven't been migrated yet (functions_worker runs AFTER tables).
        Strip both so the table is created bare; ``row_filters_worker`` and
        ``column_masks_worker`` apply them after the target functions
        exist.
        """
        import re

        # Strip ``WITH ROW FILTER <fqn> ON (cols)``.
        ddl = re.sub(
            r"\s*\bWITH\s+ROW\s+FILTER\b[^(]*\([^)]*\)",
            "",
            ddl,
            flags=re.IGNORECASE,
        )
        # Strip per-column ``MASK <fqn> [USING (cols)]`` appearing inline
        # in column definitions.
        ddl = re.sub(
            r"\s+MASK\s+[^\s,()]+(?:\s+USING\s*\([^)]*\))?",
            "",
            ddl,
            flags=re.IGNORECASE,
        )
        return ddl

    def _build_create_stmt_from_columns(self, catalog: str, schema: str, name: str) -> str:
        """Synthesize ``CREATE TABLE ... USING ICEBERG`` from information_schema.

        Used as a fallback when SHOW CREATE TABLE is unsupported (UC-managed
        Iceberg). Best-effort — captures columns + nullability, omits partition
        spec / TBLPROPERTIES (iceberg migration is already Option A — lossy).
        """
        rows = self.spark.sql(  # type: ignore[attr-defined]
            f"""
            SELECT column_name, full_data_type, is_nullable
            FROM `{catalog}`.`information_schema`.`columns`
            WHERE table_schema = '{schema}' AND table_name = '{name}'
            ORDER BY ordinal_position
            """
        ).collect()
        if not rows:
            return ""
        col_defs = []
        for r in rows:
            null_clause = "" if (r.is_nullable or "").upper() == "YES" else " NOT NULL"
            col_defs.append(f"  `{r.column_name}` {r.full_data_type}{null_clause}")
        cols_sql = ",\n".join(col_defs)
        return f"CREATE TABLE `{catalog}`.`{schema}`.`{name}` (\n{cols_sql}\n) USING ICEBERG"

    def get_function_ddl(self, function_fqn: str) -> str:
        """Return the full CREATE OR REPLACE FUNCTION statement.

        Queries information_schema.routines + parameters to reconstruct the DDL,
        since DESCRIBE FUNCTION only returns the body.

        Supports SQL UDFs and Python UDFs. Python UDFs are wrapped with
        ``AS $$...$$`` instead of ``RETURN ...`` (which is SQL-UDF syntax).
        """
        parts = function_fqn.strip("`").split("`.`")
        if len(parts) != 3:
            raise ValueError(f"Malformed function FQN: {function_fqn}")
        catalog, schema, name = parts

        routine = self.spark.sql(  # type: ignore[attr-defined]
            f"""
            SELECT specific_name, data_type, routine_body, routine_definition, external_language
            FROM `{catalog}`.`information_schema`.`routines`
            WHERE routine_schema = '{schema}' AND routine_name = '{name}'
            LIMIT 1
            """
        ).first()
        if routine is None:
            raise ValueError(f"Function not found in information_schema: {function_fqn}")

        params = self.spark.sql(  # type: ignore[attr-defined]
            f"""
            SELECT parameter_name, data_type, ordinal_position
            FROM `{catalog}`.`information_schema`.`parameters`
            WHERE specific_schema = '{schema}' AND specific_name = '{routine.specific_name}'
              AND parameter_mode = 'IN'
            ORDER BY ordinal_position
            """
        ).collect()
        param_sig = ", ".join(f"{p.parameter_name} {p.data_type}" for p in params)

        body = (routine.routine_definition or "").strip()
        is_external = (routine.routine_body or "").upper() == "EXTERNAL"
        language = (routine.external_language or "").upper() if routine.external_language else ""

        # Python UDF: LANGUAGE PYTHON + AS $$...$$ body wrapper
        if is_external and language == "PYTHON":
            env_clause = self._fetch_python_udf_environment(function_fqn)
            env_piece = f"{env_clause} " if env_clause else ""
            return (
                f"CREATE OR REPLACE FUNCTION {function_fqn}({param_sig}) "
                f"RETURNS {routine.data_type} "
                f"LANGUAGE PYTHON "
                f"{env_piece}"
                f"AS $$\n{body}\n$$"
            )

        # Other external languages (rare): LANGUAGE X + AS $$...$$ for safety
        if is_external and language:
            return (
                f"CREATE OR REPLACE FUNCTION {function_fqn}({param_sig}) "
                f"RETURNS {routine.data_type} "
                f"LANGUAGE {language} "
                f"AS $$\n{body}\n$$"
            )

        # SQL UDF — existing path
        return f"CREATE OR REPLACE FUNCTION {function_fqn}({param_sig}) RETURNS {routine.data_type} RETURN {body}"

    def _fetch_python_udf_environment(self, function_fqn: str) -> str | None:
        """Return the literal ``ENVIRONMENT (...)`` clause for a Python
        UDF, or ``None`` if the function has no environment spec or
        ``SHOW CREATE FUNCTION`` isn't available on this runtime.

        ``information_schema.routines`` doesn't expose the ENVIRONMENT
        dependencies list, so we parse it out of ``SHOW CREATE FUNCTION``
        which renders the full recreate-DDL when the function has one.
        Replay of a Python UDF without its ENVIRONMENT drops its pip
        dependencies silently on target — the UDF still creates but
        fails at first invocation with ModuleNotFoundError.
        """
        try:
            rows = self.spark.sql(f"SHOW CREATE FUNCTION {function_fqn}").collect()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 — older runtimes or edge cases
            return None
        if not rows:
            return None
        # SHOW CREATE FUNCTION returns a single row with one string column.
        first = rows[0]
        ddl = first[0] if len(first) > 0 else ""
        if not ddl:
            return None
        m = re.search(r"ENVIRONMENT\s*\(", ddl, re.IGNORECASE)
        if not m:
            return None
        # Balance parens starting at the '(' opened by ENVIRONMENT.
        start = m.end()
        depth = 1
        idx = start
        while idx < len(ddl) and depth > 0:
            ch = ddl[idx]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            idx += 1
        if depth != 0:
            return None
        return "ENVIRONMENT (" + ddl[start:idx]

    # ------------------------------------------------------------------
    # Functions & volumes
    # ------------------------------------------------------------------

    def list_functions(self, catalog: str, schema: str) -> list[str]:
        """List UDFs in a schema via information_schema.routines."""
        query = (
            f"SELECT routine_name FROM `{catalog}`.`information_schema`.`routines` WHERE routine_schema = '{schema}'"
        )
        rows = self.spark.sql(query).collect()  # type: ignore[attr-defined]
        return [f"`{catalog}`.`{schema}`.`{row.routine_name}`" for row in rows]

    def list_volumes(self, catalog: str, schema: str) -> list[dict]:
        """List volumes in a schema via information_schema.volumes.

        Includes ``storage_location`` for both managed and external volumes
        so the volume_worker can register EXTERNAL targets at the same path
        and can locate source bytes for MANAGED volume copies.
        """
        query = (
            f"SELECT volume_name, volume_type, storage_location "
            f"FROM `{catalog}`.`information_schema`.`volumes` "
            f"WHERE volume_schema = '{schema}'"
        )
        rows = self.spark.sql(query).collect()  # type: ignore[attr-defined]
        return [
            {
                "fqn": f"`{catalog}`.`{schema}`.`{row.volume_name}`",
                "volume_type": row.volume_type,
                "storage_location": row.storage_location,
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Phase 3: governance objects
    #
    # Each helper returns a list of dicts. Discovery wraps each dict into a
    # discovery_inventory row with the appropriate object_type; per-type
    # fields that don't fit the normalized columns go into metadata_json.
    # REST-based helpers (policies, monitors, online_tables) wrap API calls
    # in try/except so discovery keeps running on workspaces where a
    # preview feature is not enabled.
    # ------------------------------------------------------------------

    def list_catalog_tags(self, catalog: str) -> list[dict]:
        """Catalog-level tags for one catalog. Called ONCE per catalog by
        discovery — separate from ``list_tags`` (which is per-schema) so a
        catalog tag isn't emitted once per schema. Finding #7: the old code
        queried ``catalog_tags`` inside ``list_tags(catalog, schema)`` (whose
        WHERE filters only on ``catalog_name``), so a catalog with N schemas
        produced N duplicate catalog-tag rows that then collided in the
        discovery MERGE (#8)."""
        results: list[dict] = []
        with _suppress():
            rows = self.spark.sql(  # type: ignore[attr-defined]
                f"SELECT catalog_name, tag_name, tag_value "
                f"FROM system.information_schema.catalog_tags "
                f"WHERE catalog_name = '{catalog}'"
            ).collect()
            for r in rows:
                results.append(
                    {
                        "securable_type": "CATALOG",
                        "securable_fqn": f"`{catalog}`",
                        "tag_name": r.tag_name,
                        "tag_value": r.tag_value,
                    }
                )
        return results

    def list_tags(self, catalog: str, schema: str) -> list[dict]:
        """List SCHEMA/TABLE/COLUMN/VOLUME tags in one schema via
        system.information_schema. Catalog-level tags are handled separately
        by ``list_catalog_tags`` (once per catalog) — see finding #7.

        Aggregates schema/table/column/volume tags into a single normalized
        shape so tags_worker can replay them with one dispatch.
        """
        results: list[dict] = []

        with _suppress():
            rows = self.spark.sql(  # type: ignore[attr-defined]
                f"SELECT schema_name, tag_name, tag_value "
                f"FROM system.information_schema.schema_tags "
                f"WHERE catalog_name = '{catalog}' AND schema_name = '{schema}'"
            ).collect()
            for r in rows:
                results.append(
                    {
                        "securable_type": "SCHEMA",
                        "securable_fqn": f"`{catalog}`.`{schema}`",
                        "tag_name": r.tag_name,
                        "tag_value": r.tag_value,
                    }
                )

        with _suppress():
            rows = self.spark.sql(  # type: ignore[attr-defined]
                f"SELECT table_name, tag_name, tag_value "
                f"FROM system.information_schema.table_tags "
                f"WHERE catalog_name = '{catalog}' AND schema_name = '{schema}'"
            ).collect()
            for r in rows:
                results.append(
                    {
                        "securable_type": "TABLE",
                        "securable_fqn": f"`{catalog}`.`{schema}`.`{r.table_name}`",
                        "tag_name": r.tag_name,
                        "tag_value": r.tag_value,
                    }
                )

        with _suppress():
            rows = self.spark.sql(  # type: ignore[attr-defined]
                f"SELECT table_name, column_name, tag_name, tag_value "
                f"FROM system.information_schema.column_tags "
                f"WHERE catalog_name = '{catalog}' AND schema_name = '{schema}'"
            ).collect()
            for r in rows:
                results.append(
                    {
                        "securable_type": "COLUMN",
                        "securable_fqn": f"`{catalog}`.`{schema}`.`{r.table_name}`",
                        "column_name": r.column_name,
                        "tag_name": r.tag_name,
                        "tag_value": r.tag_value,
                    }
                )

        with _suppress():
            rows = self.spark.sql(  # type: ignore[attr-defined]
                f"SELECT volume_name, tag_name, tag_value "
                f"FROM system.information_schema.volume_tags "
                f"WHERE catalog_name = '{catalog}' AND schema_name = '{schema}'"
            ).collect()
            for r in rows:
                results.append(
                    {
                        "securable_type": "VOLUME",
                        "securable_fqn": f"`{catalog}`.`{schema}`.`{r.volume_name}`",
                        "tag_name": r.tag_name,
                        "tag_value": r.tag_value,
                    }
                )

        return results

    def list_row_filters(self, catalog: str, schema: str) -> list[dict]:
        """Row filters applied to tables via ALTER TABLE ... SET ROW FILTER.

        Uses the UC Tables API (authoritative) instead of
        ``information_schema.tables.row_filter_name`` — the latter column
        doesn't surface on every runtime and silently returns empty,
        which means discovery misses filters and migrate_row_filters has
        nothing to replay.
        """
        results: list[dict] = []
        try:
            tables = self.auth_manager.source_client.tables.list(  # type: ignore[attr-defined]
                catalog_name=catalog,
                schema_name=schema,
            )
        except Exception:  # noqa: BLE001
            return results
        for t in tables:
            rf = getattr(t, "row_filter", None)
            if rf is None:
                continue
            fn_name = getattr(rf, "function_name", None) or ""
            input_cols = getattr(rf, "input_column_names", None) or []
            results.append(
                {
                    "table_fqn": f"`{catalog}`.`{schema}`.`{t.name}`",
                    "filter_function_fqn": fn_name,
                    "filter_columns": list(input_cols),
                }
            )
        return results

    def list_column_masks(self, catalog: str, schema: str) -> list[dict]:
        """Column masks applied via ALTER TABLE ... ALTER COLUMN ... SET MASK.

        Uses the UC Tables API (authoritative) — ``information_schema
        .columns.mask_name`` isn't reliably populated on every runtime.
        """
        results: list[dict] = []
        try:
            tables = self.auth_manager.source_client.tables.list(  # type: ignore[attr-defined]
                catalog_name=catalog,
                schema_name=schema,
            )
        except Exception:  # noqa: BLE001
            return results
        for t in tables:
            for col in getattr(t, "columns", None) or []:
                mask = getattr(col, "mask", None)
                if mask is None:
                    continue
                fn_name = getattr(mask, "function_name", None) or ""
                using_cols = getattr(mask, "using_column_names", None) or []
                results.append(
                    {
                        "table_fqn": f"`{catalog}`.`{schema}`.`{t.name}`",
                        "column_name": col.name,
                        "mask_function_fqn": fn_name,
                        "mask_using_columns": list(using_cols),
                    }
                )
        return results

    def list_policies(self, securables: list[tuple[str, str]] | None = None) -> list[dict]:
        """ABAC policies attached to any of *securables*.

        The UC ABAC API is per-securable — there is no workspace-level
        "list everything" endpoint. Callers pass a list of
        ``(securable_type, full_name)`` tuples (``securable_type`` one
        of ``CATALOG`` / ``SCHEMA`` / ``TABLE``), and we iterate them.
        Each entry carries the full policy JSON under ``definition`` so
        the policies worker can POST it back on target verbatim.

        A missing endpoint (preview not enabled) is a benign skip. But a
        PERMISSION_DENIED is surfaced as a WARNING (not silently swallowed):
        the migration principal running discovery must be able to READ ABAC
        policies (own the securable or be a metastore admin), otherwise
        policy-protected tables go undetected and could be migrated with
        silent data loss (same class as the #6 connections swallow). A
        WARNING lets the operator see detection may be incomplete without
        failing the whole run over one unreadable securable.
        """
        if not securables:
            return []
        results: list[dict] = []
        try:
            client = self.auth_manager.source_client.api_client  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            return []
        seen: set[str] = set()
        perm_denied: list[str] = []
        for sec_type, full_name in securables:
            try:
                # ABAC policies are addressed by PATH params, not query params:
                # GET /api/2.1/unity-catalog/policies/{securable_type}/{fullname}.
                # The query-param form returns "No API found" — that 404 was
                # being swallowed here, which is why policies never appeared in
                # discovery (root cause of the always-NONE policy coverage).
                resp = client.do(
                    "GET",
                    f"/api/2.1/unity-catalog/policies/{sec_type}/{full_name}",
                )
            except PermissionDenied:
                perm_denied.append(f"{sec_type} {full_name}")
                continue
            except Exception:  # noqa: BLE001 — preview absent / no API, benign skip
                continue
            if not isinstance(resp, dict):
                continue
            for p in resp.get("policies", []):
                # Policy names are unique per securable; across securables
                # a (securable_fullname, name) tuple is the real key.
                key = f"{p.get('on_securable_fullname', '')}::{p.get('name', '')}"
                if key in seen:
                    continue
                seen.add(key)
                results.append(
                    {
                        "policy_name": p.get("name"),
                        "securable_type": p.get("on_securable_type") or sec_type,
                        "securable_fqn": p.get("on_securable_fullname") or full_name,
                        "definition": p,
                    }
                )
        if perm_denied:
            logger.warning(
                "ABAC policy read denied on %d securable(s): %s. Policy-protected "
                "table detection may be INCOMPLETE — the migration principal must be "
                "able to read ABAC policies (own the securable or be a metastore "
                "admin). Affected tables could otherwise be migrated with silent data loss.",
                len(perm_denied),
                sorted(perm_denied),
            )
        return results

    def list_monitors(self, table_fqns: list[str]) -> list[dict]:
        """Lakehouse monitors via REST per-table GET.

        Iterates over the given table FQNs (batched by caller); 404s are
        skipped (table has no monitor).
        """
        results: list[dict] = []
        client = self.auth_manager.source_client.api_client  # type: ignore[attr-defined]
        for fqn in table_fqns:
            clean = fqn.strip("`").replace("`.`", ".")
            try:
                resp = client.do("GET", f"/api/2.1/unity-catalog/tables/{clean}/monitor")
                if isinstance(resp, dict) and resp.get("table_name"):
                    results.append(
                        {
                            "table_fqn": fqn,
                            "definition": resp,
                        }
                    )
            except Exception:  # noqa: BLE001
                continue  # no monitor (404) or transient error
        return results

    def _model_aliases(self, model_fqn: str) -> dict[str, str]:
        """Return a model's alias map ``{alias_name: version}`` via MLflow.

        Finding #23: on the runtime SDK (0.49.0) the version-level and
        model-level ``.aliases`` fields read EMPTY even when an alias exists
        — only MLflow's model-level alias map is reliable (verified live:
        ``registered_models.get().aliases`` == [] but
        ``MlflowClient().get_registered_model(fqn).aliases`` ==
        ``{"champion": "1"}``). ``mlflow`` is runtime-only, imported lazily;
        unit tests replace this method via the class seam.
        """
        from mlflow.tracking import MlflowClient

        rm = MlflowClient(registry_uri="databricks-uc").get_registered_model(model_fqn)
        return {name: str(ver) for name, ver in (getattr(rm, "aliases", {}) or {}).items()}

    def list_registered_models(self, catalog: str, schema: str) -> list[dict]:
        """Registered models in a schema via SDK ``registered_models.list``.

        Returns one record per model with its versions inline — artifact
        copy happens in the models worker, this helper just captures the
        metadata. Aliases are sourced from the MLflow model-level alias map
        (``_model_aliases``) and keyed onto their version, because the SDK
        ``.aliases`` fields read empty on the runtime (finding #23).
        """
        results: list[dict] = []
        try:
            client = self.auth_manager.source_client  # type: ignore[attr-defined]
            for m in client.registered_models.list(catalog_name=catalog, schema_name=schema):
                versions = []
                if m.full_name is None:
                    continue
                # alias map {alias_name: version} for this model (finding #23).
                alias_map: dict[str, str] = {}
                with _suppress():
                    alias_map = self._model_aliases(m.full_name)
                with _suppress():
                    for v in client.model_versions.list(full_name=m.full_name):
                        _ver = str(v.version)
                        _aliases = [a for a, av in alias_map.items() if str(av) == _ver]
                        versions.append(
                            {
                                "version": v.version,
                                "source": v.source,
                                "storage_location": getattr(v, "storage_location", None),
                                "status": str(getattr(v, "status", "")),
                                "aliases": _aliases,
                            }
                        )
                results.append(
                    {
                        "model_fqn": m.full_name,
                        "owner": getattr(m, "owner", None),
                        "storage_location": getattr(m, "storage_location", None),
                        "comment": getattr(m, "comment", None),
                        "versions": versions,
                    }
                )
        except Exception:  # noqa: BLE001
            return []
        return results

    def list_connections(self) -> list[dict]:
        """UC connections via SDK. Workspace-level.

        Passwords in ``options`` are not returned by the GET API — worker
        will record a partial status and surface the list of credentials
        the customer must re-enter.

        Raises: underlying SDK exceptions (``PermissionDenied``, SDK method
        renames, etc.) **propagate** so a misconfigured run fails loudly
        rather than silently producing an empty connection inventory
        (finding #6 — the old blanket ``except: return []`` masked both the
        SDK ``.list`` rename and permission errors, so connections were
        silently skipped with no trace). Mirrors ``list_foreign_catalogs``.
        Note: a principal that simply lacks ``USE CONNECTION`` sees an empty
        list (no error) — that visibility gap is closed by the SPN grant
        recipe (user_guide Step 4), not here.
        """
        results: list[dict] = []
        client = self.auth_manager.source_client  # type: ignore[attr-defined]
        for c in client.connections.list():
            _ct = getattr(c, "connection_type", "")
            # Store the clean value ("SQLSERVER"), not the enum repr
            # ("ConnectionType.SQLSERVER") — the latter breaks the worker's
            # ConnectionType coercion. ``.value`` for an enum, else str.
            results.append(
                {
                    "connection_name": c.name,
                    "connection_type": getattr(_ct, "value", str(_ct)),
                    "options": dict(getattr(c, "options", {}) or {}),
                    "comment": getattr(c, "comment", None),
                }
            )
        return results

    def list_foreign_catalogs(self) -> list[dict]:
        """Foreign catalogs — federated catalogs backed by a UC connection.

        Result is memoized on the instance because ``discovery.py`` and
        ``list_foreign_catalog_names()`` both call this in the same run.

        Raises:
            Underlying SDK exceptions (``PermissionDenied`` etc.)
            propagate so a misconfigured run fails loudly rather than
            silently producing an empty foreign-catalog inventory.
        """
        if self._foreign_catalogs_cache is not None:
            return self._foreign_catalogs_cache
        results: list[dict] = []
        client = self.auth_manager.source_client  # type: ignore[attr-defined]
        for c in client.catalogs.list(include_browse=True):
            ctype = str(getattr(c, "catalog_type", ""))
            if "FOREIGN" in ctype.upper():
                results.append(
                    {
                        "catalog_name": c.name,
                        "connection_name": getattr(c, "connection_name", None),
                        "options": dict(getattr(c, "options", {}) or {}),
                        "comment": getattr(c, "comment", None),
                    }
                )
        self._foreign_catalogs_cache = results
        return results

    def list_foreign_catalog_names(self) -> set[str]:
        """Names of FOREIGN_CATALOG entries — used by discovery to exclude
        them from schema/table iteration. They are captured separately via
        ``list_foreign_catalogs`` as governance metadata; iterating their
        schemas via ``information_schema.tables`` fails because the query
        is routed to the remote JDBC source which lacks UC columns like
        ``data_source_format``.
        """
        return {fc["catalog_name"] for fc in self.list_foreign_catalogs()}

    def list_online_tables(self) -> list[dict]:
        """Online tables via REST ``/api/2.0/online-tables``.

        Workspace-level. Returns empty list if preview is not enabled.
        """
        results: list[dict] = []
        try:
            client = self.auth_manager.source_client.api_client  # type: ignore[attr-defined]
            resp = client.do("GET", "/api/2.0/online-tables")
            for t in resp.get("online_tables", []) if isinstance(resp, dict) else []:
                results.append(
                    {
                        "online_table_fqn": t.get("name"),
                        "source_table_fqn": (t.get("spec") or {}).get("source_table_full_name"),
                        "definition": t,
                    }
                )
        except Exception:  # noqa: BLE001
            return []
        return results

    def list_shares(self, exclude_names: frozenset[str] = frozenset()) -> list[dict]:
        """Delta shares the migration SPN has visibility on.

        UC's ``shares.list()`` only returns shares where the caller is
        the owner or has ``USE SHARE``. Shares the SPN can't see are
        silently skipped by the API itself — not raised here — so a
        missing share means the operator hasn't granted the SPN access
        (see README: "Delta Sharing prerequisites"). The outer try/
        except defends against a server-side hiccup; it logs a
        ``print`` warning rather than silently returning empty so
        operators aren't left wondering why no migration_status row
        ever shows up for a share they know exists.

        Excludes the migration's own internal share ``cp_migration_share``
        (callers pass it via ``exclude_names``).
        """
        results: list[dict] = []
        try:
            client = self.auth_manager.source_client  # type: ignore[attr-defined]
            # SDK compat: SharesAPI.list() was renamed to list_shares() in
            # newer databricks-sdk (the pin is >=0.30, unbounded). Calling the
            # missing name raises AttributeError, which the outer except below
            # swallowed → shares ALWAYS came back empty (recipients use the
            # still-present .list(), which is why recipient migrated but share
            # never did). Prefer the new name, fall back to the old.
            _share_lister = getattr(client.shares, "list_shares", None) or client.shares.list
            for s in _share_lister():  # type: ignore[attr-defined]
                if s.name in exclude_names:
                    continue
                objects = []
                with _suppress():
                    full = client.shares.get(name=s.name, include_shared_data=True)
                    for o in full.objects or []:
                        objects.append(
                            {
                                "name": o.name,
                                "data_object_type": str(o.data_object_type),
                                "shared_as": getattr(o, "shared_as", None),
                            }
                        )
                results.append(
                    {
                        "share_name": s.name,
                        "comment": getattr(s, "comment", None),
                        "objects": objects,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[list_shares] shares.list() failed ({exc!r}); returning "
                f"empty list. The migration SPN may lack USE SHARE / "
                f"ownership on any source share — see README."
            )
            return []
        return results

    def list_recipients(self, exclude_prefix: str = "cp_migration_recipient_") -> list[dict]:
        """Delta Sharing recipients the migration SPN has visibility on.

        Same access semantics as ``list_shares``: UC returns only
        recipients the caller owns or holds USE RECIPIENT on.
        Skips our internal cp_migration_recipient_*.
        """
        results: list[dict] = []
        try:
            client = self.auth_manager.source_client  # type: ignore[attr-defined]
            for r in client.recipients.list():
                if r.name and r.name.startswith(exclude_prefix):
                    continue
                results.append(
                    {
                        "recipient_name": r.name,
                        "authentication_type": str(getattr(r, "authentication_type", "")),
                        "global_metastore_id": getattr(r, "data_recipient_global_metastore_id", None),
                        "comment": getattr(r, "comment", None),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            print(
                f"[list_recipients] recipients.list() failed ({exc!r}); "
                f"returning empty list. The migration SPN may lack USE "
                f"RECIPIENT / ownership on any source recipient — see README."
            )
            return []
        return results

    def list_providers(self) -> list[dict]:
        """Delta Sharing providers (the source's subscribed providers)."""
        results: list[dict] = []
        try:
            client = self.auth_manager.source_client  # type: ignore[attr-defined]
            for p in client.providers.list():
                results.append(
                    {
                        "provider_name": p.name,
                        "authentication_type": str(getattr(p, "authentication_type", "")),
                        "comment": getattr(p, "comment", None),
                    }
                )
        except Exception:  # noqa: BLE001
            return []
        return results

    # ------------------------------------------------------------------
    # View dependency ordering
    # ------------------------------------------------------------------

    def resolve_view_dependency_order(self, views: list[str]) -> list[str]:
        """Topological sort of views using Kahn's algorithm.

        Dependencies are extracted from ``information_schema.views.view_definition``
        by regex-matching three-part names in the SQL body. Databricks UC does
        NOT ship ``information_schema.view_table_usage`` as of Apr 2026, so we
        parse the view body instead. The regex is intentionally conservative —
        any dependency we miss is caught by the retry loop in views_worker.
        If cycles are detected, remaining views are appended at the end.
        """
        view_set = set(views)

        # Build adjacency: edge from dependency -> view (dependency must come first)
        in_degree: dict[str, int] = {v: 0 for v in views}
        dependents: dict[str, list[str]] = defaultdict(list)

        # Group views by catalog so we issue one information_schema.views query
        # per catalog rather than one per view.
        by_catalog: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        for view in views:
            parts = view.strip("`").split("`.`")
            if len(parts) != 3:
                continue
            catalog, schema, name = parts
            by_catalog[catalog].append((schema, name, view))

        for catalog, triples in by_catalog.items():
            # view_definition is NULL unless the current user owns the view —
            # that's acceptable: a non-owned view's deps we just can't see, so
            # it gets in_degree=0 and the retry loop picks up the slack.
            query = (
                f"SELECT table_schema, table_name, view_definition "
                f"FROM `{catalog}`.`information_schema`.`views` "
                f"WHERE table_schema IN ({_sql_in_literal(set(s for s, _, _ in triples))})"
            )
            try:
                rows = self.spark.sql(query).collect()  # type: ignore[attr-defined]
            except Exception:
                rows = []
            row_map = {(r.table_schema, r.table_name): (r.view_definition or "") for r in rows}
            for schema, name, view in triples:
                body = row_map.get((schema, name), "")
                if not body:
                    continue
                for dep_fqn in _extract_three_part_refs(body, view_set):
                    if dep_fqn != view:
                        dependents[dep_fqn].append(view)
                        in_degree[view] += 1

        # Kahn's algorithm
        queue: deque[str] = deque(v for v in views if in_degree[v] == 0)
        ordered: list[str] = []

        while queue:
            node = queue.popleft()
            ordered.append(node)
            for dependent in dependents.get(node, []):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        # Cycle handling – append any remaining views that weren't resolved
        if len(ordered) < len(views):
            remaining = [v for v in views if v not in set(ordered)]
            ordered.extend(remaining)

        return ordered

    # ------------------------------------------------------------------
    # Grants
    # ------------------------------------------------------------------

    def list_grants(self, securable_type: str, securable_fqn: str) -> list[dict]:
        """Return grants on a securable object."""
        rows = self.spark.sql(  # type: ignore[attr-defined]
            f"SHOW GRANTS ON {securable_type} {securable_fqn}"
        ).collect()
        return [
            {
                "principal": row.Principal,
                "action_type": row.ActionType,
                "securable_type": row.ObjectType,
                "securable_fqn": row.ObjectKey,
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Legacy Hive Metastore (Phase 2)
    # ------------------------------------------------------------------

    def list_hive_databases(self) -> list[str]:
        """Return non-default databases under hive_metastore."""
        rows = self.spark.sql(f"SHOW DATABASES IN {HIVE_CATALOG}").collect()  # type: ignore[attr-defined]
        # The first column can be `databaseName` or `namespace` depending on DBR.
        out: list[str] = []
        for row in rows:
            name = getattr(row, "databaseName", None) or getattr(row, "namespace", None)
            if name and name not in EXCLUDED_SCHEMAS:
                out.append(name)
        return out

    def _describe_hive_table(self, database: str, table: str) -> dict:
        """Parse ``DESCRIBE EXTENDED hive_metastore.db.t`` into a summary dict.

        Returns a dict with: ``table_type`` (MANAGED/EXTERNAL/VIEW),
        ``storage_location`` (raw), ``provider`` (delta/parquet/...),
        ``comment`` if present.
        """
        rows = self.spark.sql(  # type: ignore[attr-defined]
            f"DESCRIBE EXTENDED `{HIVE_CATALOG}`.`{database}`.`{table}`"
        ).collect()
        info: dict = {}
        in_detail = False
        for row in rows:
            col = (row.col_name or "").strip()
            data = (row.data_type or "").strip() if row.data_type is not None else ""
            if col == "# Detailed Table Information":
                in_detail = True
                continue
            if not in_detail:
                continue
            key = col.rstrip(":").lower().replace(" ", "_")
            if not key:
                continue
            info[key] = data
        return {
            "table_type": info.get("type", "").upper(),
            "storage_location": info.get("location", ""),
            "provider": (info.get("provider") or "").lower(),
            "comment": info.get("comment", ""),
        }

    def classify_hive_tables(self, database: str) -> list[dict]:
        """List all tables in a Hive database and classify each into a migration category.

        Returns records with: ``fqn``, ``object_type`` (hive_table / hive_view),
        ``table_type``, ``storage_location``, ``provider``, ``data_category``
        (hive_managed_dbfs_root / hive_managed_nondbfs / hive_external / hive_view).
        """
        rows = self.spark.sql(  # type: ignore[attr-defined]
            f"SHOW TABLES IN `{HIVE_CATALOG}`.`{database}`"
        ).collect()
        results: list[dict] = []
        for row in rows:
            table_name = row.tableName
            try:
                details = self._describe_hive_table(database, table_name)
            except Exception:  # noqa: BLE001
                # Some tables (e.g. temp views from other sessions) may fail to describe.
                continue
            fqn = f"`{HIVE_CATALOG}`.`{database}`.`{table_name}`"
            data_category = self.categorize_hive_table(details)
            obj_type = "hive_view" if details["table_type"] == "VIEW" else "hive_table"
            results.append(
                {
                    "fqn": fqn,
                    "object_type": obj_type,
                    "table_type": details["table_type"],
                    "storage_location": details["storage_location"],
                    "provider": details["provider"],
                    "data_category": data_category,
                }
            )
        return results

    @staticmethod
    def categorize_hive_table(details: dict) -> str:
        """Map DESCRIBE output to a migration category.

        Categories drive which Phase 2 worker handles the table.
        """
        table_type = (details.get("table_type") or "").upper()
        location = (details.get("storage_location") or "").lower()

        if table_type == "VIEW":
            return "hive_view"
        if table_type == "EXTERNAL":
            return "hive_external"
        if table_type == "MANAGED":
            # DBFS root = /user/hive/warehouse/... or /user/... under dbfs:/
            if location.startswith("dbfs:/user/hive/warehouse/") or location.startswith("dbfs:/user/spark-warehouse/"):
                return "hive_managed_dbfs_root"
            if location.startswith(("dbfs:/mnt/", "abfss://", "s3://", "gs://", "wasbs://")):
                return "hive_managed_nondbfs"
            # Fallback: unknown dbfs path, treat as dbfs_root (safer — triggers data copy)
            if location.startswith("dbfs:/"):
                return "hive_managed_dbfs_root"
            return "hive_managed_nondbfs"
        # Unknown type (shouldn't happen for Hive) — classify as external to be safe.
        return "hive_external"

    def list_hive_functions(self, database: str) -> list[str]:
        """Return user-defined functions in a Hive database (fully-qualified)."""
        try:
            # SHOW USER FUNCTIONS IN <cat>.<db> requires the CURRENT CATALOG to
            # be <cat> on current runtimes ("target schema is not in the current
            # catalog"), unlike SHOW TABLES. Set the catalog on the (persistent)
            # session first, then query with the bare db name. Without this the
            # call errored and was swallowed → hive functions were never found.
            self.spark.sql(f"USE CATALOG `{HIVE_CATALOG}`")  # type: ignore[attr-defined]
            rows = self.spark.sql(  # type: ignore[attr-defined]
                f"SHOW USER FUNCTIONS IN `{database}`"
            ).collect()
        except Exception:  # noqa: BLE001
            return []
        out = []
        db_lower = database.lower()
        for row in rows:
            # `function` column name varies by DBR version — probe common ones.
            name = getattr(row, "function", None) or getattr(row, "name", None)
            if not name:
                continue
            # SHOW USER FUNCTIONS IN <db> is scoped to user functions in that
            # database (built-ins are global, not in a named db, so they don't
            # appear here). Names come back qualified (cat.db.fn / db.fn) OR bare
            # (fn) depending on DBR — keep bare names (they're user funcs in the
            # scoped db) and qualified names that belong to THIS db; drop names
            # qualified to a different db. (The old "skip if no dot" rule wrongly
            # dropped bare user functions — that's why hive_function was empty.)
            parts = name.split(".")
            if len(parts) >= 2 and parts[-2].lower() != db_lower:
                continue
            out.append(f"`{HIVE_CATALOG}`.`{database}`.`{parts[-1]}`")
        return out
