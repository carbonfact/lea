import abc
from google.cloud import bigquery

from . import views


class Client(abc.ABC):
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


class BigQuery(Client):
    def __init__(self, credentials, project_id, dataset_name, username):
        self.project_id = project_id
        self.client = bigquery.Client(credentials=credentials)
        self._dataset_name = dataset_name
        self.username = username

    @property
    def dataset_name(self):
        return f"{self._dataset_name}_{self.username}" if self.username else self._dataset_name

    def _make_job(self, view: views.SQLView):
        query = view.query
        if self.username:
            query = query.replace(f"{self._dataset_name}.", f"{self.dataset_name}.")

        return self.client.create_job(
            {
                "query": {
                    "query": query,
                    "destinationTable": {
                        "projectId": self.project_id,
                        "datasetId": self.dataset_name,
                        "tableId": f"{view.schema}__{view.name}".lstrip("_"),
                    },
                    "createDisposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            }
        )

    def _create_sql(self, view: views.SQLView):
        job = self._make_job(view)
        job.result()

    def _create_python(self, view: views.PythonView):
        output = self._load_python(view)

        job_config = bigquery.LoadJobConfig(
            schema=[],
            write_disposition="WRITE_TRUNCATE",
        )

        job = self.client.load_table_from_dataframe(
            output,
            f"{self.project_id}.{self.dataset_name}.{view.schema}__{view.name}",
            job_config=job_config,
        )
        job.result()

    def _load_sql(self, view: views.SQLView):
        job = self._make_job(view)
        return job.to_dataframe()

    def list_existing(self):
        return [
            table.table_id.split("__", 1) for table in self.client.list_tables(self.dataset_name)
        ]

    def delete(self, schema: str, view_name: str):
        self.client.delete_table(f"{self.project_id}.{self.dataset_name}.{schema}__{view_name}")
