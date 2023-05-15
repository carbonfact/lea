from __future__ import annotations

import collections
import graphlib
import io
import itertools

import lea


class DAGOfViews(graphlib.TopologicalSorter, collections.UserDict):
    def __init__(self, views: list[lea.views.View]):
        view_to_dependencies = {(view.schema, view.name): view.dependencies for view in views}
        graphlib.TopologicalSorter.__init__(self, view_to_dependencies)
        collections.UserDict.__init__(self, {(view.schema, view.name): view for view in views})
        self.dependencies = view_to_dependencies

    @property
    def schemas(self):
        return sorted(set(schema for schema, _ in self))

    def to_mermaid(self):
        out = io.StringIO()
        out.write('%%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%\n')
        out.write("flowchart TB\n")
        nodes = set(node for deps in self.dependencies.values() for node in deps) | set(
            self.dependencies.keys()
        )
        schema_nodes = itertools.groupby(sorted(nodes), lambda node: node[0])
        for schema, nodes in schema_nodes:
            out.write(f"    subgraph {schema}\n")
            for _, node in nodes:
                out.write(f"    {schema}.{node}({node})\n")
            out.write("    end\n\n")
        for dst, srcs in self.dependencies.items():
            dst = ".".join(dst)
            for src in sorted(srcs):
                src = ".".join(src)
                out.write(f"    {src} --> {dst}\n")
        return out.getvalue()
