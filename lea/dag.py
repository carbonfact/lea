import graphlib
import pathlib
from collections.abc import Iterator

from .dialects import SQLDialect
from .table_ref import TableRef
from .scripts import read_scripts, Script


class DAGOfScripts(graphlib.TopologicalSorter):

    def __init__(self, dependency_graph: dict[TableRef, set[TableRef]], scripts: list[Script], dataset_dir: pathlib.Path):
        graphlib.TopologicalSorter.__init__(self, dependency_graph)
        self.dependency_graph = dependency_graph
        self.scripts = {script.table_ref: script for script in scripts}
        self.dataset_dir = dataset_dir

        # If a test depends on a script, we want said test to become a dependency of the scripts
        # that depend on the script. This is opinionated, but it makes sense in the context of
        # data pipelines.
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
        #             if (
        #                 str(dependent_test.table_ref) == 'kaya.tests.skus_for_each_account' and
        #                 str(dependent_script.table_ref) == 'kaya.collect.data_quality_issues'
        #             ):
        #                 print(script.table_ref)
        #             dependency_graph[dependent_script.table_ref].add(dependent_test.table_ref)

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
                yield from self.iter_ancestors(table_ref)
            if include_descendants:
                yield from self.iter_descendants(table_ref)

        all_selected_table_refs = set()
        for query in queries:
            selected_table_refs = set(_select(query))
            all_selected_table_refs.update(selected_table_refs)

        return {
            table_ref for table_ref in all_selected_table_refs
            # Some nodes in the graph are not part of the views, such as external dependencies
            if table_ref in self.scripts
        }

    def iter_ancestors(self, table_ref: TableRef):
        for child in self.dependency_graph.get(table_ref, []):
            yield child
            yield from self.iter_ancestors(child)

    def iter_descendants(self, table_ref: TableRef):
        for potential_child in self.dependency_graph:
            if table_ref in self.dependency_graph[potential_child]:
                yield potential_child
                yield from self.iter_descendants(potential_child)

    def iter_scripts(self, *queries: str) -> Iterator[Script]:
        selected_table_refs = self.select(*queries)
        if not selected_table_refs:
            raise ValueError("Nothing found for queries: " + ", ".join(queries))

        for table_ref in self.get_ready():

            if table_ref not in self.scripts or table_ref not in selected_table_refs:
                self.done(table_ref)
                continue

            yield self.scripts[table_ref]
