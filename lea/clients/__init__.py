from __future__ import annotations

import getpass
import json
import os

from .bigquery import BigQuery
from .duckdb import DuckDB


def make_client(production: bool, wap_mode=False):
    warehouse = os.environ["LEA_WAREHOUSE"]
    username = None if production else str(os.environ.get("LEA_USERNAME", getpass.getuser()))

    if warehouse == "bigquery":
        # Do imports here to avoid loading them all the time
        from google.oauth2 import service_account

        scopes_str = os.environ.get("LEA_BQ_SCOPES", "https://www.googleapis.com/auth/bigquery")
        scopes = scopes_str.split(",")
        scopes = [scope.strip() for scope in scopes]

        credentials = service_account.Credentials.from_service_account_info(
            json.loads(os.environ["LEA_BQ_SERVICE_ACCOUNT"], strict=False), scopes=scopes
        )

        return BigQuery(
            credentials=credentials,
            location=os.environ["LEA_BQ_LOCATION"],
            write_project_id=os.environ["LEA_BQ_PROJECT_ID"],
            compute_project_id=os.environ.get("LEA_BQ_COMPUTE_PROJECT_ID", credentials.project_id),
            dataset_name=os.environ["LEA_BQ_DATASET_NAME"],
            username=username,
            wap_mode=wap_mode,
        )
    elif warehouse == "duckdb":
        return DuckDB(path=os.environ["LEA_DUCKDB_PATH"], username=username, wap_mode=wap_mode)
    else:
        raise ValueError(f"Unsupported warehouse: {warehouse}")
