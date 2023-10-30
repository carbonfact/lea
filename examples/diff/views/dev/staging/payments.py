from __future__ import annotations

import pathlib

import pandas as pd

here = pathlib.Path(__file__).parent
payments = pd.read_csv(
    here.parents[3] / "jaffle_shop" / "jaffle_shop" / "seeds" / "raw_payments.csv"
)
payments = payments.rename(columns={"id": "payment_id"})
payments["amount"] = payments["amount"]
