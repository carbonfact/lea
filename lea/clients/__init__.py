from __future__ import annotations

import getpass
import json
import os


def make_client(production: bool):
    warehouse = os.environ["LEA_WAREHOUSE"]
    username = None if production else str(os.environ.get("LEA_USERNAME", getpass.getuser()))

    if warehouse == "bigquery":
        # Do imports here to avoid loading them all the time
        from google.oauth2 import service_account

        from lea.clients.bigquery import BigQuery

        return BigQuery(
            credentials=service_account.Credentials.from_service_account_info(
                json.loads(os.environ["LEA_BQ_SERVICE_ACCOUNT"], strict=False)
            ),
            location=os.environ["LEA_BQ_LOCATION"],
            project_id=os.environ["LEA_BQ_PROJECT_ID"],
            dataset_name=os.environ["LEA_BQ_DATASET_NAME"],
            username=username,
        )
    elif warehouse == "duckdb":
        from lea.clients.duckdb import DuckDB

        return DuckDB(
            path=os.environ["LEA_DUCKDB_PATH"],
            username=username,
        )
    else:
        raise ValueError(f"Unsupported warehouse: {warehouse}")
