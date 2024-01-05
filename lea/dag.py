from __future__ import annotations

import collections
import graphlib
import io
import re

FOUR_SPACES = "    "


class DAGOfViews(graphlib.TopologicalSorter):
    def __init__(self, graph):
        graphlib.TopologicalSorter.__init__(self, graph)
        self.graph = graph

    @property
    def schemas(self) -> set:
        return {schema for schema, *_ in self.graph}

    @property
    def schema_dependencies(self):
        deps = collections.defaultdict(set)
        for (src_schema, *_), dsts in self.graph.items():
            deps[src_schema].update([schema for schema, *_ in dsts if schema != src_schema])
        return deps

    def list_ancestors(self, node):
        """Returns a list of all the ancestors for a given node."""

        def _list_ancestors(node):
            for child in self.graph.get(node, []):
                yield child
                yield from _list_ancestors(child)

        return list(_list_ancestors(node))

    def list_descendants(self, node):
        """Returns a list of all the descendants for a given node."""

        def _list_descendants(node):
            for parent in self.graph:
                if node in self.graph[parent]:
                    yield parent
                    yield from _list_descendants(parent)

        return list(_list_descendants(node))

    @property
    def roots(self):
        """A root is a view that doesn't depend on any other view.

        A root can depend on a table that is not part of the views, such as a third-party table. It
        can also depend on nothing at all.

        Roots are important because they are the only views that can be run without having to run
        any other view first. If we want to do a run in a git branch, we don't need to run the
        roots if they haven't been selected, because they are already available in production.

        """
        return {
            view.key
            for view in self.values()
            if all(dependency[0] not in self.schemas for dependency in self.graph[view.key])
        }

    def _select(self, query: str) -> str:

        def select(query, include_ancestors, include_descendants):
            if query.endswith("+"):
                yield from select(
                    query[:-1], include_ancestors=include_ancestors, include_descendants=True
                )
                return
            if query.startswith("+"):
                yield from select(
                    query[1:], include_ancestors=True, include_descendants=include_descendants
                )
                return
            if query.endswith("/"):
                query_key = tuple([key for key in query.split("/") if key])
                for key in self.graph:
                    if key[: len(query_key)] == query_key:
                        yield from select(
                            ".".join(key),
                            include_ancestors=include_ancestors,
                            include_descendants=include_descendants,
                        )
            else:
                key = tuple(query.split("."))
                yield key
                if include_ancestors:
                    yield from self.list_ancestors(key)
                if include_descendants:
                    yield from self.list_descendants(key)

        return {
            key
            for key in select(query, include_ancestors=False, include_descendants=False)
            # Some nodes in the graph are not part of the views, such as third-party tables
            if key in self.graph
        }

    def select(self, *queries: str) -> set:
        """Make a whitelist of tables given a query.

        These are the different queries to handle:

        schema.table
        schema.table+   (descendants)
        +schema.table   (ancestors)
        +schema.table+  (ancestors and descendants)
        schema/         (all tables in schema)
        schema/+        (all tables in schema with their descendants)
        +schema/        (all tables in schema with their ancestors)
        +schema/+       (all tables in schema with their ancestors and descendants)

        Examples
        --------

        >>> import lea

        >>> client = lea.clients.DuckDB(":memory:")
        >>> runner = lea.Runner('examples/jaffle_shop/views', client=client)
        >>> dag = runner.dag

        >>> def pprint(whitelist):
        ...     for key in sorted(whitelist):
        ...         print('.'.join(key))

        schema.table

        >>> pprint(dag.select('staging.orders'))
        staging.orders

        schema.table+ (descendants)

        >>> pprint(dag.select('staging.orders+'))
        analytics.finance.kpis
        analytics.kpis
        core.customers
        core.orders
        staging.orders

        +schema.table (ancestors)

        >>> pprint(dag.select('+core.customers'))
        core.customers
        staging.customers
        staging.orders
        staging.payments

        +schema.table+ (ancestors and descendants)

        >>> pprint(dag.select('+core.customers+'))
        analytics.kpis
        core.customers
        staging.customers
        staging.orders
        staging.payments

        schema/ (all tables in schema)

        >>> pprint(dag.select('staging/'))
        staging.customers
        staging.orders
        staging.payments

        schema/+ (all tables in schema with their descendants)

        >>> pprint(dag.select('staging/+'))
        analytics.finance.kpis
        analytics.kpis
        core.customers
        core.orders
        staging.customers
        staging.orders
        staging.payments

        +schema/ (all tables in schema with their ancestors)

        >>> pprint(dag.select('+core/'))
        core.customers
        core.orders
        staging.customers
        staging.orders
        staging.payments

        +schema/+  (all tables in schema with their ancestors and descendants)

        >>> pprint(dag.select('+core/+'))
        analytics.finance.kpis
        analytics.kpis
        core.customers
        core.orders
        staging.customers
        staging.orders
        staging.payments

        schema/subschema/

        >>> pprint(dag.select('analytics/finance/'))
        analytics.finance.kpis

        """

        def _expand_query(query):
            # It's possible to query views via git. For example:
            # * `git` will select all the views that have been modified compared to the main branch.
            # * `git+` will select all the modified views, and their descendants.
            # * `+git` will select all the modified views, and their ancestors.
            # * `+git+` will select all the modified views, with their ancestors and descendants.
            if m := re.match(r"(?P<ancestors>\+?)git(?P<descendants>\+?)", query):
                ancestors = m.group("ancestors") == "+"
                descendants = m.group("descendants") == "+"

                repo = git.Repo(".")  # TODO: is using "." always correct? Probably not.
                # Changes that have been committed
                staged_diffs = repo.index.diff(
                    repo.refs.main.commit
                    # repo.remotes.origin.refs.main.commit
                )
                # Changes that have not been committed
                unstage_diffs = repo.head.commit.diff(None)

                for diff in staged_diffs + unstage_diffs:
                    # We only care about changes to views
                    # TODO: here we only check the file's location. We don't check whether the file
                    # is actually a view or not.
                    # One thing to note is that we don't filter out deleted views. This is because
                    # these views will get filtered out by dag.select anyway.
                    diff_path = pathlib.Path(diff.a_path)
                    if diff_path.is_relative_to(self.views_dir):
                        view = lea.views.open_view_from_path(
                            diff_path, self.views_dir, self.client.sqlglot_dialect
                        )
                        yield ("+" if ancestors else "") + str(view) + ("+" if descendants else "")
            else:
                yield query

        if not queries:
            return set(self.graph.keys())

        return set.union(*(self._select(q) for query in queries for q in _expand_query(query)))

    @property
    def _nested_schema(self):
        """

        >>> import pathlib
        >>> import lea
        >>> from pprint import pprint

        >>> client = lea.clients.DuckDB(":memory:")
        >>> runner = lea.Runner(
        ...     views_dir=pathlib.Path(__file__).parent.parent / "examples" / "jaffle_shop" / "views",
        ...     client=client
        ... )
        >>> dag = runner.dag

        >>> pprint(dag._nested_schema)
        {'analytics': {'finance': {'kpis': {}}, 'kpis': {}},
         'core': {'customers': {}, 'orders': {}},
         'staging': {'customers': {}, 'orders': {}, 'payments': {}}}

        """

        nodes = set(node for deps in self.graph.values() for node in deps) | set(self.graph.keys())

        nested_schema = {}

        for key in nodes:
            current_level = nested_schema
            for part in key:
                if part not in current_level:
                    current_level[part] = {}
                current_level = current_level[part]

        return nested_schema

    def _to_mermaid_views(self):
        out = io.StringIO()
        out.write('%%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%\n')
        out.write("flowchart TB\n")

        def output_subgraph(schema: str, values: dict, prefix: str = ""):
            out.write(f"\n{FOUR_SPACES}subgraph {schema}\n")
            for value in sorted(values.keys()):
                sub_values = values[value]
                path = f"{prefix}.{schema}" if prefix else schema
                full_path = f"{path}.{value}"
                if sub_values:
                    output_subgraph(value, sub_values, path)
                else:
                    out.write(f"{FOUR_SPACES*2}{full_path}({value})\n")
            out.write(f"{FOUR_SPACES}end\n\n")

        # Print out the nodes, within each subgraph block
        nested_schema = self._nested_schema
        for schema in sorted(nested_schema.keys()):
            values = nested_schema[schema]
            output_subgraph(schema, values)

        # Print out the edges
        for dst, srcs in sorted(self.graph.items()):
            dst = ".".join(dst)
            for src in sorted(srcs):
                src = ".".join(src)
                out.write(f"{FOUR_SPACES}{src} --> {dst}\n")

        return out.getvalue()

    def _to_mermaid_schemas(self):
        out = io.StringIO()
        out.write('%%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%\n')
        out.write("flowchart TB\n")
        schema_dependencies = self.schema_dependencies
        nodes = set(node for deps in schema_dependencies.values() for node in deps) | set(
            schema_dependencies.keys()
        )
        for node in sorted(nodes):
            out.write(f"{FOUR_SPACES}{node}({node})\n")
        for dst, srcs in sorted(schema_dependencies.items()):
            for src in sorted(srcs):
                out.write(f"{FOUR_SPACES}{src} --> {dst}\n")
        return out.getvalue()

    def to_mermaid(self, schemas_only=False):
        """

        >>> import pathlib
        >>> import lea

        >>> client = lea.clients.DuckDB(":memory:")
        >>> runner = lea.Runner(
        ...     views_dir=pathlib.Path(__file__).parent.parent / "examples" / "jaffle_shop" / "views",
        ...     client=client
        ... )
        >>> dag = runner.dag

        >>> print(dag.to_mermaid(schemas_only=True))
        %%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%
        flowchart TB
            analytics(analytics)
            core(core)
            staging(staging)
            core --> analytics
            staging --> core
        <BLANKLINE>

        >>> print(dag.to_mermaid())
        %%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%
        flowchart TB
        <BLANKLINE>
            subgraph analytics
        <BLANKLINE>
            subgraph finance
                analytics.finance.kpis(kpis)
            end
        <BLANKLINE>
                analytics.kpis(kpis)
            end
        <BLANKLINE>
        <BLANKLINE>
            subgraph core
                core.customers(customers)
                core.orders(orders)
            end
        <BLANKLINE>
        <BLANKLINE>
            subgraph staging
                staging.customers(customers)
                staging.orders(orders)
                staging.payments(payments)
            end
        <BLANKLINE>
            core.orders --> analytics.finance.kpis
            core.customers --> analytics.kpis
            core.orders --> analytics.kpis
            staging.customers --> core.customers
            staging.orders --> core.customers
            staging.payments --> core.customers
            staging.orders --> core.orders
            staging.payments --> core.orders
        <BLANKLINE>

        """
        if schemas_only:
            return self._to_mermaid_schemas()
        return self._to_mermaid_views()
