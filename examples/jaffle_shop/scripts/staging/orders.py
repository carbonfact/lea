"""Docstring for the orders view."""
from __future__ import annotations

import pathlib

import pandas as pd

here = pathlib.Path(__file__).parent
orders = pd.read_csv(here.parent.parent / "jaffle_shop" / "seeds" / "raw_orders.csv")
orders = orders.rename(columns={"id": "order_id", "user_id": "customer_id"})
