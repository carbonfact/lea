"""Docstring for the customers view."""

from __future__ import annotations

import pathlib

import pandas as pd

here = pathlib.Path(__file__).parent
customers = pd.read_csv(
    here.parents[3] / "jaffle_shop" / "jaffle_shop" / "seeds" / "raw_customers.csv"
)
customers = customers.rename(columns={"id": "customer_id"})
