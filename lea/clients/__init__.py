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

        service_account_info_path = os.environ["LEA_BQ_SERVICE_ACCOUNT"]
        with open(service_account_info_path, "r") as f:
            service_account_info = json.load(f)

        return BigQuery(
            credentials=service_account.Credentials.from_service_account_info(
                service_account_info, scopes=scopes
            ),
            location=os.environ["LEA_BQ_LOCATION"],
            project_id=os.environ["LEA_BQ_PROJECT_ID"],
            dataset_name=os.environ["LEA_BQ_DATASET_NAME"],
            username=username,
            wap_mode=wap_mode,
        )
    elif warehouse == "duckdb":
        return DuckDB(path=os.environ["LEA_DUCKDB_PATH"], username=username, wap_mode=wap_mode)
    else:
        raise ValueError(f"Unsupported warehouse: {warehouse}")
