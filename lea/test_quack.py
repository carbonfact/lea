from __future__ import annotations

import tempfile

import duckdb
import pytest

from lea.databases import DuckLakeClient, TableStats
from lea.dialects import BigQueryDialect, DuckDBDialect
from lea.quack import classify_scripts, determine_deps_to_pull, transpile_query
from lea.scripts import SQLScript
from lea.session import Session
from lea.table_ref import TableRef

DUMMY_TABLE_STATS = TableStats(n_rows=0, n_bytes=0, updated_at=None)


def make_ref(schema: str, name: str, dataset: str = "ds", project: str | None = None) -> TableRef:
    return TableRef(dataset=dataset, schema=(schema,), name=name, project=project)


def make_script(table_ref: TableRef, code: str = "SELECT 1") -> SQLScript:
    return SQLScript(
        table_ref=table_ref,
        code=code,
        sql_dialect=BigQueryDialect(),
    )


# --- classify_scripts tests ---


class TestClassifyScripts:
    def test_script_with_external_dep_is_native(self):
        """A script that depends on an external source must be native."""
        ext = make_ref("raw", "external_table")
        staging = make_ref("staging", "customers")

        staging_script = make_script(staging)
        scripts = {staging: staging_script}
        dep_graph = {staging: {ext}}

        native, duck = classify_scripts(dep_graph, scripts)

        assert staging in native
        assert len(duck) == 0

    def test_script_with_only_dag_deps_is_duck(self):
        """A script whose deps are all in the DAG should be duck."""
        staging = make_ref("staging", "customers")
        core = make_ref("core", "customers")

        scripts = {
            staging: make_script(staging),
            core: make_script(core),
        }
        dep_graph = {
            staging: set(),
            core: {staging},
        }

        native, duck = classify_scripts(dep_graph, scripts)

        assert len(native) == 0
        assert staging in duck
        assert core in duck

    def test_upstream_propagation(self):
        """If script B has external dep and depends on script A, then A must also be native."""
        ext = make_ref("raw", "source")
        a = make_ref("staging", "helper")
        b = make_ref("staging", "main")
        c = make_ref("core", "output")

        scripts = {
            a: make_script(a),
            b: make_script(b),
            c: make_script(c),
        }
        dep_graph = {
            a: set(),
            b: {ext, a},
            c: {b},
        }

        native, duck = classify_scripts(dep_graph, scripts)

        assert a in native
        assert b in native
        assert c in duck

    def test_deep_upstream_propagation(self):
        """Ancestors propagate recursively."""
        ext = make_ref("raw", "source")
        a = make_ref("staging", "level1")
        b = make_ref("staging", "level2")
        c = make_ref("staging", "level3")

        scripts = {
            a: make_script(a),
            b: make_script(b),
            c: make_script(c),
        }
        dep_graph = {
            a: set(),
            b: {a},
            c: {ext, b},
        }

        native, duck = classify_scripts(dep_graph, scripts)

        assert a in native
        assert b in native
        assert c in native
        assert len(duck) == 0

    def test_diamond_dependency(self):
        """Diamond: A and B are independent, C depends on both. A has external dep."""
        ext = make_ref("raw", "source")
        a = make_ref("staging", "with_ext")
        b = make_ref("staging", "no_ext")
        c = make_ref("core", "combined")

        scripts = {
            a: make_script(a),
            b: make_script(b),
            c: make_script(c),
        }
        dep_graph = {
            a: {ext},
            b: set(),
            c: {a, b},
        }

        native, duck = classify_scripts(dep_graph, scripts)

        assert a in native
        assert b in duck
        assert c in duck

    def test_isolated_subgraphs(self):
        """Two independent subgraphs, only one touches external sources."""
        ext = make_ref("raw", "source")
        a = make_ref("staging", "reads_ext")
        b = make_ref("core", "transforms_a")
        c = make_ref("other", "independent")
        d = make_ref("analytics", "uses_c")

        scripts = {
            a: make_script(a),
            b: make_script(b),
            c: make_script(c),
            d: make_script(d),
        }
        dep_graph = {
            a: {ext},
            b: {a},
            c: set(),
            d: {c},
        }

        native, duck = classify_scripts(dep_graph, scripts)

        assert a in native
        assert b in duck
        assert c in duck
        assert d in duck

    def test_no_external_deps_all_duck(self):
        """If no script has external deps, everything is duck."""
        a = make_ref("staging", "a")
        b = make_ref("core", "b")

        scripts = {
            a: make_script(a),
            b: make_script(b),
        }
        dep_graph = {
            a: set(),
            b: {a},
        }

        native, duck = classify_scripts(dep_graph, scripts)

        assert len(native) == 0
        assert a in duck
        assert b in duck

    def test_all_have_external_deps(self):
        """If every script has external deps, everything is native."""
        ext1 = make_ref("raw", "source1")
        ext2 = make_ref("raw", "source2")
        a = make_ref("staging", "a")
        b = make_ref("staging", "b")

        scripts = {
            a: make_script(a),
            b: make_script(b),
        }
        dep_graph = {
            a: {ext1},
            b: {ext2, a},
        }

        native, duck = classify_scripts(dep_graph, scripts)

        assert a in native
        assert b in native
        assert len(duck) == 0


# --- transpile_query tests ---


class TestTranspileQuery:
    def test_bigquery_to_duckdb_basic(self):
        sql = "SELECT SAFE_DIVIDE(a, b) FROM my_table"
        result = transpile_query(sql, from_dialect="bigquery", to_dialect="duckdb")
        assert "SAFE_DIVIDE" not in result

    def test_bigquery_to_duckdb_date_functions(self):
        sql = "SELECT DATE_TRUNC(order_date, MONTH) FROM orders"
        result = transpile_query(sql, from_dialect="bigquery", to_dialect="duckdb")
        assert "order_date" in result

    def test_identity_transpile(self):
        sql = "SELECT id, name FROM users WHERE age > 30"
        result = transpile_query(sql, from_dialect="duckdb", to_dialect="duckdb")
        assert "id" in result
        assert "name" in result


# --- Dialect quack method tests ---


class TestBigQueryDialectQuack:
    def test_format_table_ref_for_duckdb(self):
        ref = TableRef(dataset="my_dataset", schema=("staging",), name="customers", project=None)
        result = BigQueryDialect().format_table_ref_for_duckdb(ref)
        assert result == "bq.my_dataset.staging__customers"

    def test_format_table_ref_for_duckdb_with_audit_suffix(self):
        ref = TableRef(dataset="my_dataset", schema=("staging",), name="customers___audit", project=None)
        result = BigQueryDialect().format_table_ref_for_duckdb(ref)
        assert result == "bq.my_dataset.staging__customers___audit"

    def test_quack_setup_sql(self):
        env = {"LEA_BQ_PROJECT_ID": "my-project"}
        stmts = BigQueryDialect().quack_setup_sql(env)
        assert len(stmts) == 3
        assert "INSTALL bigquery" in stmts[0]
        assert "LOAD bigquery" in stmts[1]
        assert "my-project" in stmts[2]
        assert "AS bq" in stmts[2]


# --- add_context_to_script integration tests for quack mode ---
# These test the single-pass flow for duck scripts.


def make_bq_ref(schema: str, name: str, dataset: str = "citibike", project: str = "my-project") -> TableRef:
    return TableRef(dataset=dataset, schema=(schema,), name=name, project=project)


class TestAddContextForDuckScript:
    """Test the quack-aware add_context_to_script for duck scripts."""

    def _make_session(self, scripts, selected_table_refs, native_table_refs, duck_table_refs):
        return Session(
            database_client=None,
            base_dataset="citibike",
            write_dataset="citibike_max",
            scripts=scripts,
            selected_table_refs=selected_table_refs,
            unselected_table_refs=set(),
            existing_tables={},
            existing_audit_tables={},
            quack_database_client="fake_duck_client",  # just non-None to enable quack mode
            native_table_refs=native_table_refs,
            duck_table_refs=duck_table_refs,
            native_dialect=BigQueryDialect(),
            native_dataset="citibike",
        )

    def test_duck_script_rewrites_native_dep_to_bq_extension(self):
        """A duck script depending on a native script should use bq.dataset.table."""
        staging_trips = make_bq_ref("staging", "trips")
        core_patterns = make_bq_ref("core", "trip_patterns")

        scripts = {
            staging_trips: make_script(
                staging_trips,
                "SELECT * FROM `bigquery-public-data.raw.trips`",
            ),
            core_patterns: make_script(
                core_patterns,
                "SELECT usertype, COUNT(*) AS n FROM citibike.staging__trips GROUP BY usertype",
            ),
        }

        session = self._make_session(
            scripts=scripts,
            selected_table_refs={staging_trips, core_patterns},
            native_table_refs={staging_trips},
            duck_table_refs={core_patterns},
        )

        result = session.add_context_to_script(scripts[core_patterns])

        # The native dep should reference the BQ extension
        assert "bq.citibike_max.staging__trips___audit" in result.code
        # Should NOT contain the project name with hyphens
        assert "my-project" not in result.code
        # Should be DuckDB dialect now
        assert isinstance(result.sql_dialect, DuckDBDialect)

    def test_duck_script_rewrites_duck_dep_to_duckdb_format(self):
        """A duck script depending on another duck script should use DuckDB format."""
        a = make_bq_ref("staging", "data")
        b = make_bq_ref("core", "cleaned")
        c = make_bq_ref("analytics", "summary")

        scripts = {
            a: make_script(a, "SELECT 1"),
            b: make_script(b, "SELECT * FROM citibike.staging__data"),
            c: make_script(c, "SELECT COUNT(*) FROM citibike.core__cleaned"),
        }

        session = self._make_session(
            scripts=scripts,
            selected_table_refs={a, b, c},
            native_table_refs=set(),
            duck_table_refs={a, b, c},
        )

        result = session.add_context_to_script(scripts[c])

        # Duck dep should be in DuckDB format (schema.table, no project/dataset)
        assert "core.cleaned___audit" in result.code
        assert "my-project" not in result.code
        assert "citibike_max" not in result.code

    def test_duck_script_with_mixed_deps(self):
        """A duck script with both native and duck dependencies."""
        staging = make_bq_ref("staging", "from_ext")
        helper = make_bq_ref("staging", "helper")
        core = make_bq_ref("core", "combined")

        scripts = {
            staging: make_script(staging, "SELECT * FROM `other-project.raw_data.raw__external`"),
            helper: make_script(helper, "SELECT 1 AS id"),
            core: make_script(
                core,
                "SELECT s.*, h.id FROM citibike.staging__from_ext s JOIN citibike.staging__helper h ON 1=1",
            ),
        }

        session = self._make_session(
            scripts=scripts,
            selected_table_refs={staging, helper, core},
            native_table_refs={staging},
            duck_table_refs={helper, core},
        )

        result = session.add_context_to_script(scripts[core])

        # Native dep → bq extension
        assert "bq.citibike_max.staging__from_ext___audit" in result.code
        # Duck dep → DuckDB format
        assert "staging.helper___audit" in result.code
        # No project with hyphens
        assert "my-project" not in result.code

    def test_duck_script_table_ref_keeps_native_format_with_write_context(self):
        """The script's table_ref should keep native format with write context after add_context_to_script.

        The conversion to DuckDB format happens later in run_script."""
        a = make_bq_ref("staging", "data")
        b = make_bq_ref("core", "output")

        scripts = {
            a: make_script(a, "SELECT 1"),
            b: make_script(b, "SELECT * FROM citibike.staging__data"),
        }

        session = self._make_session(
            scripts=scripts,
            selected_table_refs={a, b},
            native_table_refs=set(),
            duck_table_refs={a, b},
        )

        result = session.add_context_to_script(scripts[b])

        # table_ref should have write context (write_dataset + audit suffix)
        assert result.table_ref.dataset == "citibike_max"
        assert result.table_ref.name.endswith("___audit")

    def test_bigquery_syntax_is_transpiled_to_duckdb(self):
        """BigQuery-specific SQL syntax should be transpiled to DuckDB."""
        a = make_bq_ref("staging", "data")
        b = make_bq_ref("core", "output")

        scripts = {
            a: make_script(a, "SELECT 1"),
            b: make_script(
                b,
                "SELECT SAFE_DIVIDE(total, count) AS ratio FROM citibike.staging__data",
            ),
        }

        session = self._make_session(
            scripts=scripts,
            selected_table_refs={a, b},
            native_table_refs=set(),
            duck_table_refs={a, b},
        )

        result = session.add_context_to_script(scripts[b])

        assert "SAFE_DIVIDE" not in result.code
        assert "ratio" in result.code

    def test_native_script_is_not_modified_by_quack(self):
        """Scripts classified as native should use the normal add_context_to_script path."""
        staging = make_bq_ref("staging", "data")

        scripts = {
            staging: make_script(staging, "SELECT * FROM `other-project.raw.raw__source`"),
        }

        session = self._make_session(
            scripts=scripts,
            selected_table_refs={staging},
            native_table_refs={staging},
            duck_table_refs=set(),
        )

        result = session.add_context_to_script(scripts[staging])

        # Native scripts should keep BigQuery dialect
        assert isinstance(result.sql_dialect, BigQueryDialect)

    def test_project_with_hyphens_is_removed(self):
        """BigQuery project names with hyphens must not appear in DuckDB SQL."""
        staging = make_bq_ref("staging", "trips", project="my-gcp-project")
        core = make_bq_ref("core", "metrics", project="my-gcp-project")

        scripts = {
            staging: make_script(staging, "SELECT 1"),
            core: make_script(
                core,
                "SELECT COUNT(*) AS n FROM citibike.staging__trips",
            ),
        }

        session = Session(
            database_client=None,
            base_dataset="citibike",
            write_dataset="citibike_max",
            scripts=scripts,
            selected_table_refs={staging, core},
            unselected_table_refs=set(),
            existing_tables={},
            existing_audit_tables={},
            quack_database_client="fake",
            native_table_refs={staging},
            duck_table_refs={core},
            native_dialect=BigQueryDialect(),
            native_dataset="citibike",
        )

        result = session.add_context_to_script(scripts[core])

        assert "my-gcp-project" not in result.code
        assert "bq.citibike_max" in result.code

    def test_pulled_native_dep_uses_ducklake_format(self):
        """A pulled native dep should be referenced in DuckLake format, not BQ extension."""
        staging = make_bq_ref("staging", "trips")
        core = make_bq_ref("core", "metrics")

        scripts = {
            staging: make_script(staging, "SELECT 1"),
            core: make_script(
                core,
                "SELECT COUNT(*) AS n FROM citibike.staging__trips",
            ),
        }

        session = self._make_session(
            scripts=scripts,
            selected_table_refs={staging, core},
            native_table_refs={staging},
            duck_table_refs={core},
        )
        # Mark staging as pulled into DuckLake
        session.pulled_table_refs = {staging}

        result = session.add_context_to_script(scripts[core])

        # Should use DuckLake format, not BQ extension
        assert "staging.trips" in result.code
        assert "bq." not in result.code


# --- determine_deps_to_pull tests ---


class TestDetermineDepsToPull:
    def test_native_dep_not_in_ducklake_needs_pull(self):
        """A native dep not in DuckLake and not selected should need pulling."""
        ext = make_ref("raw", "source")
        staging = make_ref("staging", "data")
        core = make_ref("core", "output")

        scripts = {
            staging: make_script(staging),
            core: make_script(core),
        }
        dep_graph = {
            staging: {ext},
            core: {staging},
        }

        result = determine_deps_to_pull(
            table_refs_to_run={core},
            duck_table_refs={core},
            dependency_graph=dep_graph,
            scripts=scripts,
            existing_duck_tables=set(),
        )

        assert staging in result

    def test_dep_already_in_ducklake_no_pull(self):
        """A dep that already exists in DuckLake should not be pulled."""
        staging = make_ref("staging", "data")
        core = make_ref("core", "output")

        scripts = {
            staging: make_script(staging),
            core: make_script(core),
        }
        dep_graph = {
            staging: set(),
            core: {staging},
        }

        result = determine_deps_to_pull(
            table_refs_to_run={core},
            duck_table_refs={core},
            dependency_graph=dep_graph,
            scripts=scripts,
            existing_duck_tables={staging},
        )

        assert len(result) == 0

    def test_dep_in_selection_no_pull(self):
        """A dep that will be refreshed (in table_refs_to_run) should not be pulled."""
        staging = make_ref("staging", "data")
        core = make_ref("core", "output")

        scripts = {
            staging: make_script(staging),
            core: make_script(core),
        }
        dep_graph = {
            staging: set(),
            core: {staging},
        }

        result = determine_deps_to_pull(
            table_refs_to_run={staging, core},
            duck_table_refs={staging, core},
            dependency_graph=dep_graph,
            scripts=scripts,
            existing_duck_tables=set(),
        )

        assert len(result) == 0

    def test_external_dep_no_pull(self):
        """External deps (not in scripts) should not be pulled."""
        ext = make_ref("raw", "source")
        staging = make_ref("staging", "data")

        scripts = {
            staging: make_script(staging),
        }
        dep_graph = {
            staging: {ext},
        }

        result = determine_deps_to_pull(
            table_refs_to_run={staging},
            duck_table_refs={staging},
            dependency_graph=dep_graph,
            scripts=scripts,
            existing_duck_tables=set(),
        )

        assert len(result) == 0

    def test_only_considers_duck_scripts_to_run(self):
        """Only deps of duck scripts that will run should be considered."""
        staging = make_ref("staging", "data")
        core = make_ref("core", "output")

        scripts = {
            staging: make_script(staging),
            core: make_script(core),
        }
        dep_graph = {
            staging: set(),
            core: {staging},
        }

        # core is native, not duck — its deps should not be pulled
        result = determine_deps_to_pull(
            table_refs_to_run={core},
            duck_table_refs=set(),
            dependency_graph=dep_graph,
            scripts=scripts,
            existing_duck_tables=set(),
        )

        assert len(result) == 0

    def test_multiple_duck_scripts_shared_dep(self):
        """A shared dep of multiple duck scripts should only appear once."""
        staging = make_ref("staging", "data")
        a = make_ref("analytics", "report_a")
        b = make_ref("analytics", "report_b")

        scripts = {
            staging: make_script(staging),
            a: make_script(a),
            b: make_script(b),
        }
        dep_graph = {
            staging: set(),
            a: {staging},
            b: {staging},
        }

        result = determine_deps_to_pull(
            table_refs_to_run={a, b},
            duck_table_refs={a, b},
            dependency_graph=dep_graph,
            scripts=scripts,
            existing_duck_tables=set(),
        )

        assert result == {staging}


# --- list_existing_table_refs tests ---


class TestListExistingTableRefs:
    def _make_catalog(self, tables: list[tuple[str, str]]) -> str:
        """Create a temporary DuckLake catalog database with the given (schema, table) pairs."""
        tmp_dir = tempfile.mkdtemp()
        path = f"{tmp_dir}/test.ducklake"
        conn = duckdb.connect(path)
        conn.execute("""
            CREATE TABLE ducklake_schema (
                schema_id INTEGER, schema_uuid VARCHAR, begin_snapshot INTEGER,
                end_snapshot INTEGER, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN
            )
        """)
        conn.execute("""
            CREATE TABLE ducklake_table (
                table_id INTEGER, table_uuid VARCHAR, begin_snapshot INTEGER,
                end_snapshot INTEGER, schema_id INTEGER, table_name VARCHAR,
                path VARCHAR, path_is_relative BOOLEAN
            )
        """)
        schemas: dict[str, int] = {}
        for schema, table in tables:
            if schema not in schemas:
                sid = len(schemas) + 1
                schemas[schema] = sid
                conn.execute(
                    "INSERT INTO ducklake_schema VALUES (?, ?, 1, NULL, ?, NULL, NULL)",
                    [sid, f"uuid-{sid}", schema],
                )
            tid = len(tables)
            conn.execute(
                "INSERT INTO ducklake_table VALUES (?, ?, 1, NULL, ?, ?, NULL, NULL)",
                [tid, f"uuid-t{tid}", schemas[schema], table],
            )
        conn.close()
        return path

    def test_returns_existing_tables(self):
        catalog = self._make_catalog([("core", "trips"), ("analytics", "metrics")])
        client = DuckLakeClient(database_path=None, catalog_path=catalog)
        result = client.list_existing_table_refs()
        names = {(r.schema[0], r.name) for r in result}
        assert ("core", "trips") in names
        assert ("analytics", "metrics") in names

    def test_empty_catalog(self):
        catalog = self._make_catalog([])
        client = DuckLakeClient(database_path=None, catalog_path=catalog)
        assert client.list_existing_table_refs() == set()

    def test_no_catalog_path(self):
        client = DuckLakeClient(database_path=None, catalog_path=None)
        assert client.list_existing_table_refs() == set()
