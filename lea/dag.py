from __future__ import annotations

import graphlib
import pathlib
import typing
from collections.abc import Iterator

from .dialects import SQLDialect
from .table_ref import TableRef
from .scripts import read_scripts, Script


class DAGOfScripts(graphlib.TopologicalSorter):

    def __init__(self, dependency_graph: dict[TableRef, set[TableRef]], scripts: list[Script], scripts_dir: pathlib.Path, dataset_name: str):
        graphlib.TopologicalSorter.__init__(self, dependency_graph)
        self.dependency_graph = dependency_graph
        self.scripts = {script.table_ref: script for script in scripts}
        self.scripts_dir = scripts_dir
        self.dataset_name = dataset_name

    @classmethod
    def from_directory(cls, scripts_dir: pathlib.Path, sql_dialect: SQLDialect, dataset_name: str) -> DAGOfScripts:
        scripts = read_scripts(scripts_dir=scripts_dir, sql_dialect=sql_dialect, dataset_name=dataset_name)

        # Fields in the script's code may contain tags. These tags induce assertion tests, which
        # are also scripts. We need to include these assertion tests in the dependency graph.
        for script in scripts:
            scripts.extend(script.assertion_tests)

        # TODO: the following is quite slow. This is because parsing dependencies from each script
        # is slow. There are several optimizations that could be done.
        dependency_graph = {
            script.table_ref: script.dependencies
            for script in scripts
        }

        return cls(dependency_graph=dependency_graph, scripts=scripts, scripts_dir=scripts_dir, dataset_name=dataset_name)

    def select(self, *queries: str) -> set[TableRef]:

        def _select(
            query: str,
            include_ancestors: bool = False,
            include_descendants: bool = False,
        ):

            if query == "*":
                yield from self.scripts.keys()
                return

            if query.endswith("+"):
                yield from _select(
                    query=query[:-1],
                    include_ancestors=include_ancestors,
                    include_descendants=True,
                )
                return

            if query.startswith("+"):
                yield from _select(
                    query=query[1:],
                    include_ancestors=True,
                    include_descendants=include_descendants,
                )
                return

            if "/" in query:
                schema = tuple(query.strip("/").split("/"))
                for table_ref in self.dependency_graph:
                    if table_ref.schema == schema:
                        yield from _select(
                            ".".join([*table_ref.schema, table_ref.name]),
                            include_ancestors=include_ancestors,
                            include_descendants=include_descendants,
                        )
                return

            *schema, name = query.split(".")
            table_ref = TableRef(dataset=self.dataset_name, schema=tuple(schema), name=name)
            yield table_ref
            if include_ancestors:
                yield from iter_ancestors(self.dependency_graph, node=table_ref)
            if include_descendants:
                yield from iter_descendants(self.dependency_graph, node=table_ref)

        all_selected_table_refs = set()
        for query in queries:
            selected_table_refs = set(_select(query))
            all_selected_table_refs.update(selected_table_refs)

        return {
            table_ref for table_ref in all_selected_table_refs
            # Some nodes in the graph are not part of the views, such as external dependencies
            if table_ref in self.scripts
        }

    def iter_scripts(self, table_refs: set[TableRef]) -> Iterator[Script]:
        """

        This method does not have the responsibility of calling .prepare() and .done() when a
        script terminates. This is the responsibility of the caller.

        """

        for table_ref in self.get_ready():

            if (
                # The DAG contains all the scripts as well as all the dependencies of each script.
                # Not all of these dependencies are scripts. We need to filter out the non-script
                # dependencies.
                table_ref not in self.scripts
                # We also need to filter out the scripts that are not part of the selected table
                # refs.
                or table_ref not in table_refs
            ):
                self.done(table_ref)
                continue

            yield self.scripts[table_ref]


def iter_ancestors(dependency_graph: dict[typing.Hashable, set[typing.Hashable]], node: typing.Hashable):
    for child in dependency_graph.get(node, []):
        yield child
        yield from iter_ancestors(dependency_graph, child)


def iter_descendants(dependency_graph: dict[typing.Hashable, set[typing.Hashable]], node: typing.Hashable):
    for potential_child in dependency_graph:
        if node in dependency_graph[potential_child]:
            yield potential_child
            yield from iter_descendants(dependency_graph, potential_child)
