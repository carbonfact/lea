from __future__ import annotations

import collections
import graphlib
import io

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

    def select(self, query: str) -> set[tuple[str]]:
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

        def _select(query, include_ancestors, include_descendants):
            if query.endswith("+"):
                yield from _select(
                    query[:-1], include_ancestors=include_ancestors, include_descendants=True
                )
                return
            if query.startswith("+"):
                yield from _select(
                    query[1:], include_ancestors=True, include_descendants=include_descendants
                )
                return
            if query.endswith("/"):
                query_key = tuple([key for key in query.split("/") if key])
                for key in self.graph:
                    if key[: len(query_key)] == query_key:
                        yield from _select(
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
            for key in _select(query, include_ancestors=False, include_descendants=False)
            # Some nodes in the graph are not part of the views, such as third-party tables
            if key in self.graph
        }

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
            out.write(f"\n{FOUR_SPACES}subgraph {schema}\n".replace('"', ""))
            for value in sorted(values.keys()):
                sub_values = values[value]
                path = f"{prefix}.{schema}" if prefix else schema
                full_path = f"{path}.{value}"
                if sub_values:
                    output_subgraph(value, sub_values, path)
                else:
                    out.write(f"{FOUR_SPACES*2}{full_path}({value})\n".replace('"', ""))
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
                out.write(f"{FOUR_SPACES}{src} --> {dst}\n".replace('"', ""))

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
            out.write(f"{FOUR_SPACES}{node}({node})\n".replace('"', ""))
        for dst, srcs in sorted(schema_dependencies.items()):
            for src in sorted(srcs):
                out.write(f"{FOUR_SPACES}{src} --> {dst}\n".replace('"', ""))
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
