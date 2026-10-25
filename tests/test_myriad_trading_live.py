"""Myriad order entry against the real venue, read-only: run with `pytest -m live`.
Needs MYRIAD_API_KEY and MYRIAD_API_SECRET with MYRIAD_PRIVATE_KEY or
MYRIAD_ACCOUNT_ADDRESS; skips without them. It places no order and does not sign in."""
from __future__ import annotations

import asyncio

import pytest

from synpath.trading.credentials import load_credentials
from synpath.trading.myriad import MyriadTrading

CREDS = load_credentials(redact_logs=True).get("myriad")

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(CREDS is None or not CREDS.api_key or not CREDS.address,
                       reason="Myriad API key and wallet are not set"),
]


def test_the_account_reads_back():
    async def go():
        async with MyriadTrading(CREDS) as myriad:  # type: ignore[arg-type]
            return await myriad.fetch_balance(), await myriad.fetch_open_orders(), await myriad.fetch_positions()

    balance, orders, positions = asyncio.run(go())
    assert balance.total >= balance.available >= 0
    assert all(o.status.value == "open" for o in orders)
    assert all(p.venue == "myriad" for p in positions)
