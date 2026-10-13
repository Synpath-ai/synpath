"""SX Bet's account, read only, against the real venue: run with `pytest -m live`.
Needs `SX_BET_PRIVATE_KEY` and `SX_BET_API_KEY`; places no order."""
from __future__ import annotations

import asyncio

import pytest

from synpath.trading.credentials import load_credentials
from synpath.trading.sx_bet import SxBetTrading

pytestmark = pytest.mark.live

CREDS = load_credentials(redact_logs=True).get("sx_bet")


@pytest.mark.skipif(CREDS is None, reason="SX_BET_PRIVATE_KEY / SX_BET_API_KEY not set")
def test_the_account_reads():
    async def go():
        async with SxBetTrading(CREDS) as sx:
            meta = await sx.metadata()
            proxy = await sx.proxy()
            orders = await sx.fetch_open_orders()
            balance = await sx.fetch_balance() if proxy.get("deployed") else None
            return meta, proxy, orders, balance

    meta, proxy, orders, balance = asyncio.run(go())
    assert meta["domain"]["verifyingContract"] and "deployed" in proxy
    assert isinstance(orders, list)
    if balance is not None:
        assert balance.currency == "USDC" and balance.total >= 0
