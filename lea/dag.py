from __future__ import annotations

import graphlib
import pathlib
import re
from collections.abc import Iterator

import git

from .dialects import SQLDialect
from .scripts import Script, read_scripts
from .table_ref import TableRef


class DAGOfScripts(graphlib.TopologicalSorter):
    def __init__(
        self,
        dependency_graph: dict[TableRef, set[TableRef]],
        scripts: list[Script],
        scripts_dir: pathlib.Path,
        dataset_name: str,
        project_name: str,
    ):
        graphlib.TopologicalSorter.__init__(self, dependency_graph)
        self.dependency_graph = dependency_graph
        self.scripts = {script.table_ref: script for script in scripts}
        self.scripts_dir = scripts_dir
        self.dataset_name = dataset_name
        self.project_name = project_name

    @classmethod
    def from_directory(
        cls,
        scripts_dir: pathlib.Path,
        sql_dialect: SQLDialect,
        dataset_name: str,
        project_name: str,
    ) -> DAGOfScripts:
        scripts = read_scripts(
            scripts_dir=scripts_dir,
            sql_dialect=sql_dialect,
            dataset_name=dataset_name,
            project_name=project_name,
        )

        # Fields in the script's code may contain tags. These tags induce assertion tests, which
        # are also scripts. We need to include these assertion tests in the dependency graph.
        for script in scripts:
            scripts.extend(script.assertion_tests)

        # TODO: the following is quite slow. This is because parsing dependencies from each script
        # is slow. There are several optimizations that could be done.
        dependency_graph = {
            script.table_ref: {
                dependency.replace_dataset(dataset_name) for dependency in script.dependencies
            }
            for script in scripts
        }

        return cls(
            dependency_graph=dependency_graph,
            scripts=scripts,
            scripts_dir=scripts_dir,
            dataset_name=dataset_name,
            project_name=project_name,
        )

    def select(self, *queries: str) -> set[TableRef]:
        """Select a subset of the views in the DAG."""

        def _select(
            query: str,
            include_ancestors: bool = False,
            include_descendants: bool = False,
        ):
            if query == "*":
                yield from self.scripts.keys()
                return

            # It's possible to query views via git. For example:
            # * `git` will select all the views that have been modified compared to the main branch.
            # * `git+` will select all the modified views, and their descendants.
            # * `+git` will select all the modified views, and their ancestors.
            # * `+git+` will select all the modified views, with their ancestors and descendants.
            if m := re.match(r"(?P<ancestors>\+?)git(?P<descendants>\+?)", query):
                include_ancestors = include_ancestors or m.group("ancestors") == "+"
                include_descendants = include_descendants or m.group("descendants") == "+"
                for table_ref in list_table_refs_that_changed(scripts_dir=self.scripts_dir):
                    yield from _select(
                        ".".join([*table_ref.schema, table_ref.name]),
                        include_ancestors=include_ancestors,
                        include_descendants=include_descendants,
                    )
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
            table_ref = TableRef(
                dataset=self.dataset_name,
                schema=tuple(schema),
                name=name,
                project=self.project_name,
            )
            yield table_ref
            if include_ancestors:
                yield from self.iter_ancestors(node=table_ref)
            if include_descendants:
                yield from self.iter_descendants(node=table_ref)

        all_selected_table_refs = set()
        for query in queries:
            selected_table_refs = set(_select(query))
            all_selected_table_refs.update(selected_table_refs)

        return {
            table_ref
            for table_ref in all_selected_table_refs
            # Some nodes in the graph are not part of the views, such as external dependencies
            if table_ref in self.scripts
        }

    def iter_scripts(self, table_refs: set[TableRef]) -> Iterator[Script]:
        """Loop over scripts in topological order.

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

    def iter_ancestors(self, node: TableRef):
        for child in self.dependency_graph.get(node, []):
            yield child
            yield from self.iter_ancestors(node=child)

    def iter_descendants(self, node: TableRef):
        for potential_child in self.dependency_graph:
            if node in self.dependency_graph[potential_child]:
                yield potential_child
                yield from self.iter_descendants(node=potential_child)


def list_table_refs_that_changed(scripts_dir: pathlib.Path) -> set[TableRef]:
    repo = git.Repo(search_parent_directories=True)
    repo_root = pathlib.Path(repo.working_tree_dir)

    absolute_scripts_dir = scripts_dir.resolve()

    # Changes that have been committed
    staged_diffs = repo.index.diff(
        repo.refs.main.commit
        # repo.remotes.origin.refs.main.commit
    )
    # Changes that have not been committed
    unstage_diffs = repo.head.commit.diff(None)

    table_refs = set()
    for diff in staged_diffs + unstage_diffs:
        # One thing to note is that we don't filter out deleted views. This is because
        # these views will get filtered out by dag.select anyway.
        diff_path = pathlib.Path(repo_root / diff.a_path).resolve()
        if diff_path.is_relative_to(absolute_scripts_dir) and tuple(diff_path.suffixes) in {
            (".sql",),
            (".sql", ".jinja"),
        }:
            table_ref = TableRef.from_path(
                scripts_dir=scripts_dir, relative_path=diff_path.relative_to(absolute_scripts_dir)
            )
            table_refs.add(table_ref)

    return table_refs
