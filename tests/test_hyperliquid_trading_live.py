"""Hyperliquid order entry against the test network: run with `pytest -m live`.

Needs no funds and no account. A throwaway key signs a real order for a
listed testnet outcome; the venue answers that the signing wallet "does not
exist" and names the address it recovered from the signature. That address
being the throwaway key's own proves the action encoding, the hash and the
signature are what the venue computes -- one byte off and it would name a
stranger.
"""
from __future__ import annotations

import asyncio
import secrets
from decimal import Decimal

import pytest

from synpath import Hyperliquid
from synpath.trading.credentials import HyperliquidCredentials
from synpath.trading.errors import PermissionDenied
from synpath.trading.hyperliquid import HyperliquidTrading
from synpath.trading.types import OrderRequest, Side

pytestmark = pytest.mark.live


def test_the_venue_recovers_the_signing_wallet():
    with Hyperliquid(testnet=True) as hl:
        market = hl.fetch_markets(limit=1)[0]

    async def run():
        async with HyperliquidTrading(HyperliquidCredentials(private_key="0x" + secrets.token_hex(32), testnet=True)) as venue:
            with pytest.raises(PermissionDenied) as refused:
                await venue.create_order(OrderRequest(market_id=market.id, side=Side.BUY, amount=Decimal("30"),
                                                      price=Decimal("0.5")))
            return venue.signer.address.lower(), str(refused.value)

    address, message = asyncio.run(run())
    assert address in message.lower()
