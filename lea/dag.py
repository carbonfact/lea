import copy
import functools
import graphlib
import pathlib
import typing
from collections.abc import Iterator

from .dialects import SQLDialect
from .table_ref import TableRef
from .scripts import read_scripts, Script


class DAGOfScripts(graphlib.TopologicalSorter):

    def __init__(self, dependency_graph: dict[TableRef, set[TableRef]], scripts: list[Script], dataset_dir: pathlib.Path):

        # If a test depends on a script, we want said test to become a dependency of the scripts
        # that depend on the script. This is opinionated, but it makes sense in the context of
        # data pipelines.

        # augmented_dependency_graph = copy.deepcopy(dependency_graph)

        # def get_ancestors(table_ref: TableRef) -> set[TableRef]:
        #     return set(iter_ancestors(augmented_dependency_graph, table_ref))

        # for script in scripts:
        #     dependent_tests = [
        #         child
        #         for child in scripts
        #         if script.table_ref in child.dependencies
        #         and child.is_test
        #     ]
        #     if not dependent_tests:
        #         continue
        #     dependent_scripts = [
        #         child
        #         for child in scripts
        #         if script.table_ref in child.dependencies
        #         and not child.is_test
        #     ]
        #     for dependent_script in dependent_scripts:
        #         for dependent_test in dependent_tests:
        #             if dependent_script.table_ref not in get_ancestors(dependent_test.table_ref):
        #                 augmented_dependency_graph[dependent_script.table_ref].add(dependent_test.table_ref)

        graphlib.TopologicalSorter.__init__(self, dependency_graph)
        self.dependency_graph = dependency_graph
        self.scripts = {script.table_ref: script for script in scripts}
        self.dataset_dir = dataset_dir

    @classmethod
    def from_directory(cls, dataset_dir: pathlib.Path, sql_dialect: SQLDialect):
        scripts = read_scripts(dataset_dir=dataset_dir, sql_dialect=sql_dialect)

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

        return cls(dependency_graph=dependency_graph, scripts=scripts, dataset_dir=dataset_dir)

    def __getitem__(self, table_ref: TableRef) -> Script:
        return self.scripts[table_ref]

    def __setitem__(self, table_ref: TableRef, script: Script):
        self.scripts[table_ref] = script

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
            table_ref = TableRef(dataset=self.dataset_dir.name, schema=tuple(schema), name=name)
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

        for table_ref in self.get_ready():

            if table_ref not in self.scripts or table_ref not in table_refs:
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
