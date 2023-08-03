from __future__ import annotations

import abc
import importlib

from lea import views


class Client(abc.ABC):
    """

    This is the base class for all clients. It defines the interface that all clients must
    implement. It is not meant to be used directly. Instead, use one of the subclasses.

    """

    def prepare(self):
        ...

    @abc.abstractproperty
    def sqlglot_dialect(self):
        ...

    @abc.abstractmethod
    def _create_sql(self, view: views.SQLView):
        ...

    @abc.abstractmethod
    def _create_python(self, view: views.PythonView):
        ...

    def create(self, view: views.View):
        if isinstance(view, views.SQLView):
            return self._create_sql(view)
        elif isinstance(view, views.PythonView):
            return self._create_python(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def _load_sql(self, view: views.SQLView):
        ...

    def _load_python(self, view: views.PythonView):
        # HACK
        # mod = importlib.import_module("views")
        # output = getattr(mod, view.name).main()
        # return output

        module_name = view.path.stem
        spec = importlib.util.spec_from_file_location(module_name, view.path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Step 2: Retrieve the variable from the module's namespace
        dataframe = getattr(module, view.name, None)
        if dataframe is None:
            raise ValueError(f"Could not find variable {view.name} in {view.path}")
        return dataframe

    def load(self, view: views.View):
        if isinstance(view, views.SQLView):
            return self._load_sql(view)
        elif isinstance(view, views.PythonView):
            return self._load_python(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def delete_view(self, view: views.View):
        ...


    @abc.abstractmethod
    def list_existing_view_names(self) -> list[tuple[str, str]]:
        ...

    @abc.abstractmethod
    def get_diff_summary(self, origin: str, destination: str):
        ...
