from __future__ import annotations

import sqlglot

from lea.scripts import Script
from lea.table_ref import TableRef


def classify_scripts(
    dependency_graph: dict[TableRef, set[TableRef]],
    scripts: dict[TableRef, Script],
) -> tuple[set[TableRef], set[TableRef]]:
    """Classify scripts into native and duck sets for quack mode.

    A script must run on the native DB if:
    1. It has a direct dependency that is NOT a DAG script (external source), OR
    2. It is an upstream dependency (ancestor) of such a script — because that script
       needs its results to be available in the native DB.

    Everything else can run on DuckDB.

    This encourages users to put external dependencies early in the DAG (staging layer),
    minimizing the native DB footprint.

    """
    native_refs: set[TableRef] = set()

    # Step 1: Find scripts with direct external dependencies
    for table_ref in scripts:
        deps = dependency_graph.get(table_ref, set())
        if any(dep not in scripts for dep in deps):
            native_refs.add(table_ref)

    # Step 2: Propagate upstream — all ancestors of native scripts must also be native
    def add_ancestors(ref: TableRef):
        for dep in dependency_graph.get(ref, set()):
            if dep in scripts and dep not in native_refs:
                native_refs.add(dep)
                add_ancestors(dep)

    for ref in list(native_refs):
        add_ancestors(ref)

    # Step 3: Everything else is duck
    duck_refs = set(scripts) - native_refs
    return native_refs, duck_refs


def determine_deps_to_pull(
    table_refs_to_run: set[TableRef],
    duck_table_refs: set[TableRef],
    dependency_graph: dict[TableRef, set[TableRef]],
    scripts: dict[TableRef, Script],
    existing_duck_tables: set[TableRef] | None = None,
) -> set[TableRef]:
    """Determine which dependencies need to be pulled into DuckLake.

    A dependency needs pulling if:
    1. It's a dependency of a duck script that will run
    2. It's not being run itself (not in table_refs_to_run)
    3. It's a DAG script (not an external source)
    4. It doesn't already exist in DuckLake (if existing_duck_tables is provided)

    """
    deps_to_pull: set[TableRef] = set()
    for table_ref in table_refs_to_run & duck_table_refs:
        for dep in dependency_graph.get(table_ref, set()):
            if dep in table_refs_to_run:
                continue
            if dep not in scripts:
                continue
            deps_to_pull.add(dep)

    if existing_duck_tables is not None:
        # existing_duck_tables are in DuckDB format (no dataset/project),
        # so normalize deps for comparison
        existing_normalized = {
            ref.replace_dataset(None).replace_project(None)
            for ref in existing_duck_tables
        }
        deps_to_pull = {
            dep for dep in deps_to_pull
            if dep.replace_dataset(None).replace_project(None) not in existing_normalized
        }

    return deps_to_pull


def transpile_query(code: str, from_dialect: str, to_dialect: str = "duckdb") -> str:
    """Transpile SQL from one dialect to another using SQLGlot."""
    statements = sqlglot.transpile(code, read=from_dialect, write=to_dialect)
    return ";\n".join(statements)


