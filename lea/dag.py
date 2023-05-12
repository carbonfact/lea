import collections
import graphlib
import io
import itertools
import lea


class DAGOfViews(graphlib.TopologicalSorter, collections.UserDict):
    def __init__(self, views: list[lea.views.View]):
        view_to_dependencies = {(view.schema, view.name): view.dependencies for view in views}
        graphlib.TopologicalSorter.__init__(self, view_to_dependencies)
        collections.UserDict.__init__(self, view_to_dependencies)

    def to_mermaid(self):
        out = io.StringIO()
        out.write('%%{init: {"flowchart": {"defaultRenderer": "elk"}} }%%\n')
        out.write("flowchart TB\n")
        nodes = set(node for deps in self.values() for node in deps) | set(self.keys())
        schema_nodes = itertools.groupby(sorted(nodes), lambda node: node[0])
        for schema, nodes in schema_nodes:
            out.write(f"    subgraph {schema}\n")
            for _, node in nodes:
                out.write(f"    {schema}.{node}({node})\n")
            out.write("    end\n\n")
        for dst, srcs in self.items():
            dst = ".".join(dst)
            for src in srcs:
                src = ".".join(src)
                out.write(f"    {src} --> {dst}\n")
        return out.getvalue()
