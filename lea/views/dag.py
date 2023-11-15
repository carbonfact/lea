from __future__ import annotations

import collections
import graphlib
import io

import lea

FOUR_SPACES = "    "


class DAGOfViews(graphlib.TopologicalSorter, collections.UserDict):
    def __init__(self, views: list[lea.views.View]):
        view_to_dependencies = {view.key: view.dependencies for view in views}
        graphlib.TopologicalSorter.__init__(self, view_to_dependencies)
        collections.UserDict.__init__(self, {view.key: view for view in views})
        self.dependencies = view_to_dependencies

    @property
    def schemas(self) -> set:
        return set(schema for schema, *_ in self)

    @property
    def schema_dependencies(self):
        deps = collections.defaultdict(set)
        for (src_schema, *_), dsts in self.dependencies.items():
            deps[src_schema].update([schema for schema, *_ in dsts if schema != src_schema])
        return deps

    def list_ancestors(self, node):
        """Returns a list of all the ancestors for a given node."""

        def _list_ancestors(node):
            for child in self.dependencies.get(node, []):
                yield child
                yield from _list_ancestors(child)

        return list(_list_ancestors(node))

    def list_descendants(self, node):
        """Returns a list of all the descendants for a given node."""

        def _list_descendants(node):
            for parent in self.dependencies:
                if node in self.dependencies[parent]:
                    yield parent
                    yield from _list_descendants(parent)

        return list(_list_descendants(node))

    def _build_nested_schema(self, deps):
        nested_schema = {}

        for path in deps:
            current_level = nested_schema
            for part in path:
                if part not in current_level:
                    current_level[part] = {}
                current_level = current_level[part]

        return nested_schema

    def _to_mermaid_subgraphs(self, out, nested_schema: dict):
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

        for schema in sorted(nested_schema.keys()):
            values = nested_schema[schema]
            output_subgraph(schema, values)

    def _to_mermaid_views(self):
        out = io.StringIO()
        out.write('%%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%\n')
        out.write("flowchart TB\n")
        nodes = set(node for deps in self.dependencies.values() for node in deps) | set(
            self.dependencies.keys()
        )

        nested_schema = self._build_nested_schema(nodes)
        self._to_mermaid_subgraphs(out, nested_schema)

        for dst, srcs in sorted(self.dependencies.items()):
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

        >>> views_dir = pathlib.Path(__file__).parent.parent.parent / "examples" / "jaffle_shop" / "views"
        >>> views = lea.views.load_views(views_dir, sqlglot_dialect="duckdb")
        >>> views = [view for view in views if view.schema not in {"tests"}]
        >>> dag = lea.views.DAGOfViews(views)

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
