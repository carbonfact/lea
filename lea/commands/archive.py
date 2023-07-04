import datetime as dt
import pathlib
import time

import concurrent.futures

import lea
import rich.console


def archive(
    views_dir: str,
    view: str
):

    from google.oauth2 import service_account

    client = lea.clients.BigQuery(
        credentials=service_account.Credentials.from_service_account_info(
            json.loads(os.environ["CARBONFACT_SERVICE_ACCOUNT"])
        ),
        project_id="carbonfact-gsheet",
        location="EU",
        dataset_name="archive",
        username=None,
    )

    view = {(view.schema, view.name): view for view in lea.views.load_views(views_dir)}[
        schema, view_name
    ]

    today = dt.date.today()
    archive_view = lea.views.GenericSQLView(
        schema="",
        name=f"kaya__{view.schema}__{view.name}__{today.strftime('%Y_%m_%d')}",
        query=f"SELECT * FROM kaya.{view.schema}__{view.name}",  # HACK
    )
    client.create(archive_view)
