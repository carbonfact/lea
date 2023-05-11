


class DAGOfViews(nx.DiGraph):
    def __init__(self, views: list[View] = None):
        super().__init__(
            (dependency, (view.schema, view.name))
            for view in views or []
            for dependency in view.dependencies
        )
        # Some views have no dependencies but still have to be included
        for view in views or []:
            self.add_node((view.schema, view.name))
