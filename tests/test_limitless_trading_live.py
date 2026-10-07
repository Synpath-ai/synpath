"""Limitless order entry against the real venue, read-only: run with
`pytest -m live`. Needs LIMITLESS_PRIVATE_KEY, LIMITLESS_API_TOKEN_ID and
LIMITLESS_API_SECRET; skips without them. It places no order."""
from __future__ import annotations

import asyncio

import pytest

from synpath.trading.credentials import load_credentials
from synpath.trading.limitless import LimitlessTrading

CREDS = load_credentials(redact_logs=True).get("limitless")

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(CREDS is None, reason="Limitless credentials are not set"),
]


def test_the_account_reads_back():
    async def go():
        async with LimitlessTrading(CREDS) as lmts:  # type: ignore[arg-type]
            profile = await lmts.profile()
            return profile, await lmts.fetch_balance(), await lmts.fetch_open_orders(), await lmts.fetch_positions()

    profile, balance, orders, positions = asyncio.run(go())
    assert profile.get("id") and balance.total >= balance.available >= 0
    assert all(o.status.value == "open" for o in orders)
    assert all(p.venue == "limitless" for p in positions)
