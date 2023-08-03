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
        mod = importlib.import_module("views")
        output = getattr(mod, view.name).main()
        return output

    def load(self, view: views.View):
        if isinstance(view, views.SQLView):
            return self._load_sql(view)
        elif isinstance(view, views.PythonView):
            return self._load_python(view)
        raise ValueError(f"Unhandled view type: {view.__class__.__name__}")

    @abc.abstractmethod
    def list_existing(self, schema: str) -> list[str]:
        ...

    @abc.abstractmethod
    def delete(self, view_name: str):
        ...
