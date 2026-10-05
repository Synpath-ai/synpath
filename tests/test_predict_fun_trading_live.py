"""predict.fun order entry against the real venue, read-only: run with
`pytest -m live`. Needs PREDICT_FUN_PRIVATE_KEY (and, on mainnet,
PREDICT_FUN_API_KEY); skips without it. It logs in -- which accepts
predict.fun's Terms of Service, as logging in on the site does -- and reads
the account. It places no order."""
from __future__ import annotations

import asyncio

import pytest

from synpath.trading.credentials import load_credentials
from synpath.trading.predict_fun import PredictFunTrading

CREDS = load_credentials(redact_logs=True).get("predict_fun")

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(CREDS is None, reason="PREDICT_FUN_PRIVATE_KEY is not set"),
]


def test_the_account_reads_back():
    async def go():
        async with PredictFunTrading(CREDS) as pf:  # type: ignore[arg-type]
            token = await pf.jwt()
            return token, await pf.fetch_balance(), await pf.fetch_open_orders(), await pf.fetch_positions()

    token, balance, orders, positions = asyncio.run(go())
    assert token and balance.total >= balance.available >= 0
    assert all(o.status.value == "open" for o in orders)
    assert all(p.venue == "predict_fun" for p in positions)
