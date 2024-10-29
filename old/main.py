from __future__ import annotations

from lea.cli import make_app
from lea.clients import make_client

app = make_app(make_client=make_client)
