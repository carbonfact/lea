from __future__ import annotations

import concurrent.futures
import dataclasses
import datetime as dt
import re
import threading
import time
from collections.abc import Callable

import lea
from lea.databases import DatabaseClient, DuckLakeClient, TableStats, Warehouse
from lea.dialects import SQLDialect
from lea.field import FieldTag
from lea.job import Job, JobStatus
from lea.scripts import Script
from lea.table_ref import TableRef


class ScriptError(Exception):
    """Raised when a script fails during execution. Already logged by monitor_job."""

    def __init__(self, table_ref: TableRef):
        self.table_ref = table_ref


class Session:
    def __init__(
        self,
        database_client: DatabaseClient | None,
        base_dataset: str,
        write_dataset: str,
        scripts: dict[TableRef, Script],
        selected_table_refs: set[TableRef],
        unselected_table_refs: set[TableRef],
        existing_tables: dict[TableRef, TableStats],
        existing_audit_tables: dict[TableRef, TableStats],
        incremental_field_name: str | None = None,
        incremental_field_values: list[str] | None = None,
        # Quack mode fields
        quack_database_client: DuckLakeClient | None = None,
        native_table_refs: set[TableRef] | None = None,
        duck_table_refs: set[TableRef] | None = None,
        native_dialect: SQLDialect | None = None,
        native_dataset: str | None = None,
        quack_extension_setup_stmts: list[str] | None = None,
        max_workers: int | None = None,
    ):
        self.database_client = database_client
        self.base_dataset = base_dataset
        self.write_dataset = write_dataset
        self.scripts = scripts
        self.selected_table_refs = selected_table_refs
        self.unselected_table_refs = unselected_table_refs
        self.existing_tables = existing_tables
        self.existing_audit_tables = existing_audit_tables
        self.incremental_field_name = incremental_field_name
        self.incremental_field_values = incremental_field_values

        # Quack mode: dual-client support
        self.quack_database_client = quack_database_client
        self.native_table_refs = native_table_refs or set()
        self.duck_table_refs = duck_table_refs or set()
        self.native_dialect = native_dialect
        self.native_dataset = native_dataset
        self.pulled_table_refs: set[TableRef] = set()
        self._quack_extension_setup_stmts = quack_extension_setup_stmts or []
        self._quack_extension_loaded = False

        self.jobs: list[Job] = []
        self.started_at = dt.datetime.now()
        self.ended_at: dt.datetime | None = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.run_script_futures: dict = {}
        self.run_script_futures_complete: dict = {}
        self.promote_audit_tables_futures: dict = {}
        self.stop_event = threading.Event()

        if self.incremental_field_name is not None:
            self.filterable_table_refs = {
                table_ref.replace_dataset(self.write_dataset)
                for table_ref in scripts
                if any(
                    field.name == incremental_field_name
                    for field in scripts[table_ref].fields or []
                )
            }
            self.incremental_table_refs = {
                table_ref.replace_dataset(self.write_dataset).remove_audit_suffix()
                for table_ref in selected_table_refs | set(existing_audit_tables)
                if any(
                    field.name == incremental_field_name and FieldTag.INCREMENTAL in field.tags
                    for field in scripts[
                        table_ref.remove_audit_suffix().replace_dataset(self.base_dataset)
                    ].fields
                    or []
                )
            }
        else:
            self.filterable_table_refs = set()
            self.incremental_table_refs = set()

    def add_write_context_to_table_ref(self, table_ref: TableRef) -> TableRef:
        table_ref = table_ref.replace_dataset(self.write_dataset)
        table_ref = table_ref.add_audit_suffix()
        return table_ref

    def remove_write_context_from_table_ref(self, table_ref: TableRef) -> TableRef:
        table_ref = table_ref.replace_dataset(self.base_dataset)
        table_ref = table_ref.remove_audit_suffix()
        return table_ref

    def _is_dep_audited(self, dependency: TableRef) -> bool:
        """Check if a dependency should use the audit table (WAP pattern)."""
        dep_base = dependency.replace_dataset(self.base_dataset)
        return (
            dep_base
            in self.selected_table_refs
            | {
                self.remove_write_context_from_table_ref(table_ref)
                for table_ref in self.existing_audit_tables
            }
            and dep_base in self.scripts
        )

    def add_context_to_script(self, script: Script) -> Script:
        # In quack mode, duck scripts get a dedicated single-pass rewriting
        if self.is_quack_mode and script.table_ref in self.duck_table_refs:
            return self._add_quack_context_to_duck_script(script)

        def add_context_to_dependency(dependency: TableRef) -> TableRef | None:
            # We don't modify the project if is has been deliberately set
            if dependency.project is not None and dependency.project != script.table_ref.project:
                return None

            if self._is_dep_audited(dependency):
                dependency = dependency.add_audit_suffix()

            dependency = dependency.replace_dataset(self.write_dataset)

            return dependency

        script = replace_script_dependencies(script=script, replace_func=add_context_to_dependency)

        # If a script is marked as incremental, it implies that it can be run incrementally. This
        # means that we have to filter the script's dependencies, as well as filter the output.
        # This logic is implemented by the script's SQL dialect.
        if script.table_ref.replace_dataset(self.write_dataset) in self.incremental_table_refs:
            script = dataclasses.replace(
                script,
                code=(
                    "".join(f"{hs};\n" for hs in script.header_statements)
                    + script.sql_dialect.add_dependency_filters(
                        code=script.query,
                        incremental_field_name=self.incremental_field_name,  # type: ignore
                        incremental_field_values=self.incremental_field_values,  # type: ignore
                        # One caveat is the dependencies which are not incremental do not have to be
                        # filtered. Indeed, they are already filtered by the fact that they are
                        # incremental.
                        dependencies_to_filter=self.filterable_table_refs
                        - self.incremental_table_refs,
                    )
                ),
            )

        # If the script is not incremental, we're not out of the woods! All scripts are
        # materialized into side-tables which we call "audit" tables. This is the WAP pattern.
        # Therefore, if a script is not incremental, but it depends on an incremental script, we
        # have to modify the script to use both the incremental and non-incremental versions of
        # the dependency. This is handled by the script's SQL dialect.
        elif self.incremental_table_refs:
            script = dataclasses.replace(
                script,
                code=script.sql_dialect.handle_incremental_dependencies(
                    code=script.code,
                    incremental_field_name=self.incremental_field_name,  # type: ignore
                    incremental_field_values=self.incremental_field_values,  # type: ignore
                    incremental_dependencies={
                        incremental_table_ref: incremental_table_ref.add_audit_suffix()
                        for incremental_table_ref in self.incremental_table_refs
                    },
                ),
            )

        return script.replace_table_ref(self.add_write_context_to_table_ref(script.table_ref))

    def _add_quack_context_to_duck_script(self, script: Script) -> Script:
        """Single-pass dependency rewriting for duck scripts in quack mode.

        Rewrites dependencies directly to their target format:
        - Native deps → read via extension (e.g. bq.dataset.schema__table___audit)
        - Duck deps → DuckDB format (e.g. schema.table___audit)
        Then transpiles the SQL and sets the script's dialect to DuckDB.

        """
        from lea.dialects import DuckDBDialect
        from lea.quack import transpile_query

        code = script.code

        for dep in script.dependencies:
            # Skip external deps (deliberately different project)
            if dep.project is not None and dep.project != script.table_ref.project:
                continue

            # Determine audit suffix
            dep_with_context = dep
            if self._is_dep_audited(dep):
                dep_with_context = dep_with_context.add_audit_suffix()

            # What the dep looks like in the original code (native format, no project)
            old_ref_str = script.sql_dialect.format_table_ref(dep.replace_project(None))

            dep_base = dep.replace_dataset(self.base_dataset)
            if dep_base in self.native_table_refs and dep_base not in self.pulled_table_refs:
                # Native dep not pulled → read via attached extension
                dep_for_ext = dep_with_context.replace_dataset(self.write_dataset)
                if self.native_dialect is None:
                    raise RuntimeError(
                        "native_dialect is required for formatting native dependency refs"
                    )
                new_ref_str = self.native_dialect.format_table_ref_for_duckdb(dep_for_ext)
            else:
                # Duck dep or pulled native dep → DuckDB format (no dataset, no project)
                duck_dep = dep_with_context.replace_dataset(None).replace_project(None)
                new_ref_str = DuckDBDialect.format_table_ref(duck_dep)

            code = re.sub(rf"\b{re.escape(old_ref_str)}\b", new_ref_str, code)

        # Transpile SQL syntax from native dialect to DuckDB
        if self.native_dialect is not None and self.native_dialect.sqlglot_dialect is not None:
            from_dialect = str(self.native_dialect.sqlglot_dialect.value)
            if from_dialect != "duckdb":
                code = transpile_query(code, from_dialect=from_dialect)

        # Keep the table_ref in native format with write context so that
        # materialize_scripts can map back to the DAG via remove_write_context_from_table_ref.
        # The conversion to DuckDB format happens later in run_script.
        write_table_ref = self.add_write_context_to_table_ref(script.table_ref)

        return dataclasses.replace(
            script, code=code, sql_dialect=DuckDBDialect(), table_ref=write_table_ref
        )

    @property
    def warehouse(self) -> Warehouse | None:
        from lea.databases import BigQueryClient, IcebergClient, MotherDuckClient

        if self.database_client is None:
            return None
        if isinstance(self.database_client, BigQueryClient):
            return Warehouse.BIGQUERY
        if isinstance(self.database_client, MotherDuckClient):
            return Warehouse.MOTHERDUCK
        if isinstance(self.database_client, IcebergClient):
            return Warehouse.ICEBERG
        if isinstance(self.database_client, DuckLakeClient):
            return Warehouse.DUCKLAKE
        return Warehouse.DUCKDB

    @property
    def is_quack_mode(self) -> bool:
        return self.quack_database_client is not None

    def format_warehouse_name(self, warehouse: Warehouse) -> str:
        """Return the warehouse name, colored only in quack mode where disambiguation matters."""
        return warehouse.rich_name if self.is_quack_mode else warehouse.display_name

    def ensure_quack_extension_loaded(self):
        """Load the native DB extension (e.g. BigQuery) into the DuckLake connection.

        Called lazily — only when we actually need to read from the native DB
        (pulling deps or running native scripts referenced via the extension).
        """
        if self._quack_extension_loaded or not self._quack_extension_setup_stmts:
            return
        import lea
        from lea.databases import Warehouse

        warehouse_name = self.format_warehouse_name(self.warehouse) if self.warehouse else "native"
        ducklake_name = self.format_warehouse_name(Warehouse.DUCKLAKE)
        lea.log.info(f"🦆 Loading {warehouse_name} extension for {ducklake_name}")
        if self.quack_database_client is None:
            raise RuntimeError("quack_database_client is required to load the native DB extension")
        conn = self.quack_database_client.connection
        for stmt in self._quack_extension_setup_stmts:
            conn.execute(stmt)
        self._quack_extension_loaded = True

    def _get_client_for_script(self, script: Script) -> DatabaseClient | DuckLakeClient:
        """In quack mode, pick the right client based on script classification."""
        if not self.is_quack_mode:
            if self.database_client is None:
                raise RuntimeError("database_client is required when not in quack mode")
            return self.database_client
        base_ref = self.remove_write_context_from_table_ref(script.table_ref)
        if base_ref in self.duck_table_refs:
            if self.quack_database_client is None:
                raise RuntimeError("quack_database_client is required for duck table refs")
            return self.quack_database_client
        if self.database_client is None:
            raise RuntimeError("database_client is required for non-duck table refs")
        return self.database_client

    def run_script(self, script: Script):
        client = self._get_client_for_script(script)

        # For duck scripts, convert table_ref to DuckDB format for DuckLake materialization
        if self.is_quack_mode and client == self.quack_database_client:
            duck_table_ref = script.table_ref.replace_dataset(None).replace_project(None)
            script = dataclasses.replace(script, table_ref=duck_table_ref)

        # If the script is a test, we don't materialize it, we just query it. A test fails if it
        # returns any rows.
        if script.is_test:
            database_job = client.query_script(script=script)
        # If the script is not a test, it's a regular table, so we materialize it. Instead of
        # directly materializing it to the destination table, we materialize it to a side-table
        # which we call an "audit" table. Once all the scripts have run successfully, we will
        # promote the audit tables to the destination tables. This is the WAP pattern.
        else:
            database_job = client.materialize_script(script=script)

        # In quack mode, label each job with the warehouse it runs on
        warehouse_label = None
        if self.is_quack_mode:
            from lea.databases import Warehouse

            if client == self.quack_database_client:
                warehouse_label = self.format_warehouse_name(Warehouse.DUCKLAKE)
            elif self.warehouse is not None:
                warehouse_label = self.format_warehouse_name(self.warehouse)

        job = Job(
            table_ref=script.table_ref,
            is_test=script.is_test,
            database_job=database_job,
            warehouse_label=warehouse_label,
        )
        self.jobs.append(job)

        msg = f"{job.status} {script.table_ref}"
        if warehouse_label:
            msg += f" ({warehouse_label})"
        if script.table_ref.remove_audit_suffix() in self.incremental_table_refs:
            msg += " (incremental)"
        lea.log.info(msg)

        self.monitor_job(job)

        if job.status == JobStatus.ERRORED:
            raise ScriptError(job.table_ref)

    def monitor_job(self, job: Job):
        # We're going to do exponential backoff. This is because we don't want to overload
        # whatever API is used to check whether a database job is over or not. We're going to
        # check every second, then every two seconds, then every four seconds, etc. until we
        # reach a maximum delay of 10 seconds.
        base_delay = 1
        max_delay = 10
        retries = 0
        checked_at = dt.datetime.now()

        while not self.stop_event.is_set():
            if not job.database_job.is_done:
                delay = min(max_delay, base_delay * (2**retries))
                retries += 1
                if (now := dt.datetime.now()) - checked_at >= dt.timedelta(seconds=10):
                    duration_str = str(now - job.started_at).split(".")[0]
                    lea.log.info(f"{job.status} {job.table_ref} after {duration_str}")
                    checked_at = now
                time.sleep(delay)
                continue

            # Case 1: the job raised an exception
            if (exception := job.database_job.exception) is not None:
                job.status = JobStatus.ERRORED
                lea.log.error(f"{job.status} {job.table_ref}\n{exception}")

            # Case 2: the job succeeded, but it's a test and there are negative cases
            elif job.is_test and not (dataframe := job.database_job.result).empty:
                job.status = JobStatus.ERRORED
                head = dataframe.head()
                n_more = len(dataframe) - len(head)
                lea.log.error(
                    f"{job.status} {job.table_ref}\n{head}"
                    + (f"({n_more} rows hidden)" if n_more else "")
                )

            # Case 3: the job succeeded!
            else:
                job.status = JobStatus.SUCCESS
                msg = f"{job.status} {job.table_ref}"
                if job.warehouse_label:
                    msg += f" ({job.warehouse_label})"
                job.ended_at = dt.datetime.now()
                # Depending on the warehouse in use, jobs may have a conclude() method, for example
                # for recording job statistics.
                job.database_job.conclude()
                duration_str = str(job.ended_at - job.started_at).split(".")[0]
                if job.ended_at - job.started_at >= dt.timedelta(seconds=1):
                    msg += f", took {duration_str}"
                if job.database_job.billed_dollars is not None:
                    msg += f", cost ${job.database_job.billed_dollars:.2f}"
                if not job.is_test:
                    if (stats := job.database_job.statistics) is not None:
                        if stats.n_rows is not None:
                            msg += f", contains {stats.n_rows:,d} rows"
                        if stats.n_bytes is not None:
                            msg += f", weighs {format_bytes(stats.n_bytes)}"
                if job.database_job.metadata:
                    msg += f" ({', '.join(job.database_job.metadata)})"
                lea.log.info(msg)

            return

    def promote_audit_table(
        self, table_ref: TableRef, client: DatabaseClient | DuckLakeClient | None = None
    ):
        if client is None:
            if self.database_client is None:
                raise RuntimeError("database_client is required to promote audit tables")
            client = self.database_client
        from_table_ref = table_ref
        to_table_ref = table_ref.remove_audit_suffix()

        is_incremental = (
            self.incremental_field_name is not None and to_table_ref in self.incremental_table_refs
        )
        if is_incremental:
            database_job = client.delete_and_insert(
                from_table_ref=from_table_ref,
                to_table_ref=to_table_ref,
                on=self.incremental_field_name,  # type: ignore
            )
        else:
            database_job = client.clone_table(
                from_table_ref=from_table_ref, to_table_ref=to_table_ref
            )

        job = Job(table_ref=to_table_ref, is_test=False, database_job=database_job)
        self.jobs.append(job)
        lea.log.info(f"{job.status} {job.table_ref}" + (" (incremental)" if is_incremental else ""))

        self.monitor_job(job)

    def end(self):
        lea.log.info("😴 Ending session")
        self.stop_event.set()
        stopped_count = 0
        for job in self.jobs:
            if job.status == JobStatus.RUNNING:
                job.database_job.stop()
                job.status = JobStatus.STOPPED
                stopped_count += 1
        if stopped_count:
            lea.log.info(f"STOPPED {stopped_count} running scripts")
        self.executor.shutdown(cancel_futures=True)
        self.ended_at = dt.datetime.now()

    @property
    def any_error_has_occurred(self) -> bool:
        return any(job.status == JobStatus.ERRORED for job in self.jobs) or any(
            future.exception() is not None for future in self.run_script_futures_complete
        )

    @property
    def total_billed_dollars(self) -> float:
        return sum(
            job.database_job.billed_dollars
            for job in self.jobs
            if job.database_job.billed_dollars is not None
        )


def replace_script_dependencies(
    script: Script, replace_func: Callable[[TableRef], TableRef | None]
) -> Script:
    """

    It's often necessary to edit the dependencies of a script. For example, we might want
    to change the dataset of a dependency. Or we might want to append a suffix a table name
    when we're doing a write/audit/publish operation.

    """
    code = script.code

    for dependency_to_edit in script.dependencies:
        new_dependency = replace_func(dependency_to_edit)
        if new_dependency is None:
            continue

        dependency_to_edit_without_project_str = script.sql_dialect.format_table_ref(
            dependency_to_edit.replace_project(None)
        )
        new_dependency_str = script.sql_dialect.format_table_ref(new_dependency)
        code = re.sub(
            rf"\b{dependency_to_edit_without_project_str}\b",
            new_dependency_str,
            code,
        )

        # We also have to handle the case where the table is referenced to access a field.
        # TODO: refactor this with the above
        dependency_to_edit_without_dataset = dataclasses.replace(
            dependency_to_edit, dataset="", project=None
        )
        dependency_to_edit_without_dataset_str = script.sql_dialect.format_table_ref(
            dependency_to_edit_without_dataset
        )
        new_dependency_without_dataset = dataclasses.replace(
            new_dependency, dataset="", project=None
        )
        new_dependency_without_dataset_str = script.sql_dialect.format_table_ref(
            new_dependency_without_dataset
        )
        code = re.sub(
            rf"\b{dependency_to_edit_without_dataset_str}\b",
            new_dependency_without_dataset_str,
            code,
        )

    return dataclasses.replace(script, code=code)


def format_bytes(size: float) -> str:
    # Define the size units in ascending order
    power = 1024
    n = 0
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]

    # Convert bytes to the highest possible unit
    while size >= power and n < len(units) - 1:
        size /= power
        n += 1

    # Format the result with two decimal places
    return f"{size:.0f}{units[n]}"
