import graphlib
import lea


class DAGOfViews(graphlib.TopologicalSorter):
    def __init__(self, views: list[lea.views.View]):
        super().__init__({
            (view.schema, view.name): view.dependencies
            for view in views
        })
