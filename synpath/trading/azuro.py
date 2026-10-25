"""Azuro bets, and the venue's bet records on the YES leg.

Built to `@azuro-org/toolkit` 7.2.0 and the Azuro backend API: bets are
signed off chain as EIP-712 messages and placed on chain by Azuro's relayer
(`azuro_signing`). Signing is checked against digests and signatures that
toolkit and viem produce.

**One credential: the wallet.** Its private key signs every bet and cash-out,
and its bet token pays for them (USDT on Polygon, WETH on Base). Before the
first bet the wallet must allow the relayer to spend the token (`approve()`,
an on-chain transaction the wallet pays gas for; `fetch_allowance()` reads it).
The relayer takes its fee, a flat amount per bet read from `/bet/gas-info`,
from the same allowance.

**A bet is a market order with a worst price.** The pool takes it whole at
the odds it quotes when the bet reaches the chain, or not at all, so every
order here is `market` (or a `limit` that is `ioc` or `fok`, which is the
same thing) and nothing rests. The price given is the worst accepted:
`buy` backs the market's YES outcome with minimum odds `1 / price`; `sell`
backs the other outcome of a two-outcome market with minimum odds
`1 / (1 - price)`. One outcome of a larger condition has no NO to sell. An
order for `n` contracts stakes `n` times the price paid, so at the quoted odds
or better it returns at least `n` (a contract pays one token).

**Placing is asynchronous.** The API answers at once with the bet order's id
in state `Created`; the relayer sends it to the chain and the order becomes
`Accepted` (filled) or `Rejected`. `create_order` returns the order pending;
`fetch_order` and `wait_for_order` follow it. A placed bet cannot be cancelled,
but where the venue offers it a bet can be cashed out before it settles
(`fetch_cashout_quote`, `cash_out`).

**A combo** backs two or more outcomes of different games in one bet, which
wins only if every leg does. It is priced by the pool, not quoted by anyone:
its odds are the legs' odds by the protocol's combo rule (`azuro_signing.combo_odds`).
`fetch_combo_quote` reads the legs' odds, states and the pool's limits on the
combo and says what stops it; `create_combo_order` signs it with a worst price
for the whole combo (minimum total odds `1 / price`) and sends it to
`/bet/orders/combo`. It is then followed like a single bet.

**Positions** net a market's accepted, unsettled single bets on the YES leg:
the contracts on the YES outcome less those on the other. Settled single bets
whose payout is still to be claimed are positions too, `resolved` with the
amount waiting in `redeemable`. A won or cancelled bet's payout stays in the
pool until it is claimed on chain: `fetch_redeemable_bets` lists them and
`redeem` sends the claims from the wallet, `CLAIM_BATCH` bets of one pool a
transaction.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from .. import ids
from ..azuro import Azuro, Chain, chain_of, game_of_condition, odds_value, outcome_odds, parse_market_id
from ..base import AsyncHttpClient, Capability
from ..errors import BadRequest, ExchangeError, MarketNotFound, NotSupported
from ..types import Page
from . import azuro_signing as sig
from .base import TradingExchange
from .credentials import AzuroCredentials
from .errors import InsufficientFunds, InvalidOrder, OrderNotFound, OrderRejected
from .limiter import BudgetLimiter, Priority
from .polymarket_signing import MAX_UINT256, WalletSigner
from .types import (
    Account, Balance, FeeEstimate, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position,
    PositionSide, Settlement, Side, TimeInForce, VENUE_ORDER_TYPES,
)

VENUE = "azuro"

DEADLINE_S = 300
"""How long a signed bet stays valid: the venue's SDK default, five minutes."""

ORDER_STATUS = {
    "Created": OrderStatus.PENDING,
    "Placed": OrderStatus.PENDING,
    "Sent": OrderStatus.PENDING,
    "Accepted": OrderStatus.CLOSED,
    "Rejected": OrderStatus.REJECTED,
    "PendingCancel": OrderStatus.PENDING_CANCEL,
    "CancelFailed": OrderStatus.CLOSED,
    "Canceled": OrderStatus.CANCELED,
    "Settled": OrderStatus.CLOSED,
}
"""A bet order's state: `Created` -> `Placed` -> `Sent` -> `Accepted` or
`Rejected`, then `Settled`. `Canceled` is the venue cancelling it (a
cancelled game, a failed relay); `CancelFailed` leaves the bet standing."""

PENDING_STATES = ("Created", "Placed", "Sent", "PendingCancel")
FILLED_STATES = ("Accepted", "CancelFailed", "Settled")

ORDERS_PAGE = 100
MAX_ORDER_PAGES = 50

MIN_COMBO_LEGS = 2
"""A combo is two or more legs. The toolkit and the SDK set no upper bound;
the pool refuses what it will not take (accepted combos of 33 legs exist)."""

CLAIM_BATCH = 50
"""Bets claimed in one `withdrawPayouts` transaction. The contract takes any
number; this bounds each transaction's gas."""

SUBGRAPH_PAGE = 100
MAX_SUBGRAPH_PAGES = 20

REDEEMABLE_QUERY = """query Redeemable($first: Int!, $skip: Int!, $where: V3_Bet_filter!) {
  v3Bets(first: $first, skip: $skip, where: $where, orderBy: createdBlockNumber, orderDirection: desc,
         subgraphError: allow) {
    id tokenId: betId actor type status result amount rawAmount odds settledOdds
    potentialPayout rawPotentialPayout payout rawPayout
    isRedeemed isRedeemable isCashedOut redeemedTxHash
    freebetId isFreebetAmountReturnable paymasterContractAddress
    createdAt: createdBlockTimestamp resolvedAt: resolvedBlockTimestamp txHash: createdTxHash
    core { address liquidityPool { address tokenDecimals } }
    selections { odds result outcome { outcomeId condition { conditionId gameId status } } }
  }
}"""
"""The SDK's `Bet` fragment, trimmed, on the bets subgraph; with the toolkit's
`Unredeemed` filter (`isRedeemable`, not cashed out) it lists what can be claimed."""


def D(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def iso_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

def market_of(condition_id: str, outcome_id: str, outcomes: list[str] | None) -> tuple[str, Side]:
    """The Synpath market a bet on this outcome is on, and its side there:
    on a two-outcome condition the lower outcome is YES (`buy`), the other NO
    (`sell`); on a larger one every outcome is its own market's YES. A
    condition whose outcomes are unknown is read as a larger one."""
    if outcomes and len(outcomes) == 2:
        return ids.qualify(VENUE, condition_id), Side.BUY if str(outcome_id) == outcomes[0] else Side.SELL
    return ids.qualify(VENUE, f"{condition_id}-{outcome_id}"), Side.BUY


def yes_price(odds: Decimal, side: Side) -> Decimal | None:
    """The YES price of a bet at these odds, on its side."""
    if odds <= 1:
        return None
    paid = Decimal(1) / odds
    return paid if side == Side.BUY else Decimal(1) - paid


def _leg(row: dict[str, Any]) -> dict[str, Any]:
    legs = row.get("conditions") or []
    return legs[0] if legs and isinstance(legs[0], dict) else {}


def _legs(row: dict[str, Any]) -> list[dict[str, Any]]:
    return [leg for leg in row.get("conditions") or [] if isinstance(leg, dict)]


def is_combo(row: dict[str, Any]) -> bool:
    return row.get("betType") == "COMBO"


def combo_legs_of(row: dict[str, Any], outcomes: dict[str, list[str]]) -> list[dict[str, Any]]:
    """A combo's legs: each one's market and side there, with the leg's odds
    and result as the venue records them."""
    out = []
    for leg in _legs(row):
        condition_id, outcome_id = str(leg.get("conditionId") or ""), str(leg.get("outcomeId") or "")
        market_id, side = market_of(condition_id, outcome_id, outcomes.get(condition_id))
        out.append({"market_id": market_id, "side": side, "condition_id": condition_id, "outcome_id": outcome_id,
                    "odds": D(leg.get("price")) if leg.get("price") not in (None, "") else None,
                    "result": leg.get("result")})
    return out


def claim_of_row(row: dict[str, Any], payout: Decimal) -> dict[str, Any]:
    """Whether a settled bet's payout is claimed, and what is left to claim:
    a won or cancelled bet, not cashed out, pays from the pool once claimed."""
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    claimed = bool(row.get("redeemedAt") or meta.get("isRedeemed"))
    payable = row.get("result") in ("Won", "Canceled") and not meta.get("isCashedOut") and payout > 0
    return {"claimed": claimed if payable else None, "claimable": payout if payable and not claimed else Decimal("0"),
            "claim_transaction": meta.get("redeemedTxHash")}


def contracts_of_row(row: dict[str, Any]) -> Decimal:
    """What the bet pays if it wins: its stake times its odds."""
    leg = _leg(row)
    if leg.get("potentialAmount") not in (None, "") and row.get("betType") != "COMBO":
        return D(leg["potentialAmount"])
    return D(row.get("amount")) * D(row.get("odds"))


def order_of_row(row: dict[str, Any], *, outcomes: list[str] | None, account: Account | None = None,
                 legs_outcomes: dict[str, list[str]] | None = None) -> Order:
    """One bet order (`GET /bet/orders/{id}`, `GET /bet/orders/bettor/...`),
    as an order on the YES leg. A combo is reported on its first leg's market
    as a buy at `1 / ` its total odds, marked `info["combo"]`, with every
    leg's market and side in `info["legs"]` (`legs_outcomes` names them)."""
    leg = _leg(row)
    market_id, side = market_of(str(leg.get("conditionId") or ""), str(leg.get("outcomeId") or ""), outcomes)
    state = str(row.get("state") or "")
    status = ORDER_STATUS.get(state, OrderStatus.PENDING)
    odds = D(row.get("odds") or leg.get("price"))
    quantity = contracts_of_row(row)
    filled = quantity if state in FILLED_STATES else Decimal("0")
    extra: dict[str, Any] = {"combo": is_combo(row)}
    if is_combo(row):
        side = Side.BUY
        extra["legs"] = combo_legs_of(row, {**(legs_outcomes or {}),
                                            str(leg.get("conditionId") or ""): outcomes or []})
    price = yes_price(odds, side)
    return Order(
        id=str(row.get("id") or ""),
        venue=VENUE,
        account=account,
        market_id=market_id,
        side=side,
        type=OrderType.MARKET,
        time_in_force=TimeInForce.FOK,
        status=status,
        price=price,
        quantity=quantity,
        filled=filled,
        remaining=quantity - filled if status in (OrderStatus.PENDING, OrderStatus.PENDING_CANCEL) else Decimal("0"),
        average_price=price if filled else None,
        cost=D(row.get("amount")),
        created_at=iso_ms(row.get("createdAt")),
        updated_at=iso_ms(row.get("updatedAt")),
        info={**row, **extra},
    )


def fill_of_row(row: dict[str, Any], *, outcomes: list[str] | None, account: Account | None = None) -> Fill | None:
    """An accepted single bet as one fill: the pool took it whole."""
    if row.get("betType") == "COMBO" or str(row.get("state") or "") not in FILLED_STATES:
        return None
    order = order_of_row(row, outcomes=outcomes, account=account)
    return Fill(
        id=str(row.get("txHash") or row.get("betId") or row.get("id") or ""),
        order_id=order.id,
        venue=VENUE,
        account=account,
        market_id=order.market_id,
        side=order.side,
        price=order.price or Decimal("0"),
        quantity=order.quantity,
        fee=None,
        liquidity=Liquidity.TAKER,
        timestamp=iso_ms(row.get("createdAt")) or 0,
        info=row,
    )


def settlement_of_row(row: dict[str, Any], *, outcomes: list[str] | None, account: Account | None = None,
                      legs_outcomes: dict[str, list[str]] | None = None) -> Settlement | None:
    """A settled bet: won, lost, or cancelled (the stake returned), with
    `info["claimed"]` (whether a payout was claimed; `None` when there is
    none) and `info["claimable"]` (what is still in the pool to claim). A
    combo is reported on its first leg's market, marked `info["combo"]`."""
    result = row.get("result")
    if result in (None, ""):
        return None
    order = order_of_row(row, outcomes=outcomes, account=account, legs_outcomes=legs_outcomes)
    cost = D(row.get("amount"))
    payout = D(row.get("payout")) if row.get("payout") is not None else (order.quantity if result == "Won" else
                                                                         cost if result == "Canceled" else Decimal("0"))
    return Settlement(
        venue=VENUE, account=account, market_id=order.market_id,
        held=PositionSide.LONG if order.side == Side.BUY else PositionSide.SHORT,
        result=str(result).lower(), won=result == "Won" if result != "Canceled" else None,
        quantity=order.quantity, cost=cost, payout=payout, pnl=payout - cost,
        timestamp=iso_ms(row.get("settledAt")) or iso_ms(row.get("updatedAt")),
        info={**row, "combo": order.info["combo"], **({"legs": order.info["legs"]} if "legs" in order.info else {}),
              **claim_of_row(row, payout)},
    )


def positions_of(rows: list[dict[str, Any]], *, outcomes: dict[str, list[str]], account: Account | None = None) -> list[Position]:
    """Accepted, unsettled single bets netted per market on the YES leg."""
    nets: dict[str, dict[str, Decimal]] = {}
    for row in rows:
        if row.get("betType") == "COMBO" or str(row.get("state") or "") not in ("Accepted", "CancelFailed") \
                or row.get("result") not in (None, ""):
            continue
        leg = _leg(row)
        condition_id = str(leg.get("conditionId") or "")
        market_id, side = market_of(condition_id, str(leg.get("outcomeId") or ""), outcomes.get(condition_id))
        entry = nets.setdefault(market_id, {"yes": Decimal("0"), "no": Decimal("0"), "cost": Decimal("0")})
        entry["yes" if side == Side.BUY else "no"] += contracts_of_row(row)
        entry["cost"] += D(row.get("amount"))
    out = []
    for market_id, entry in nets.items():
        net = entry["yes"] - entry["no"]
        out.append(Position(
            venue=VENUE, account=account, market_id=market_id,
            side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
            contracts=abs(net), inventory_yes=entry["yes"], inventory_no=entry["no"], margin=entry["cost"],
            info={"bets": "accepted, unsettled single bets"},
        ))
    return out


# ---------------------------------------------------------------------------
# Redeemable bets
# ---------------------------------------------------------------------------

def freebet_share(payout: int, amount: int, returnable: bool | None) -> int:
    """The bettor's part of a free bet's payout (the toolkit's
    `calcFreebetBettorShare`): nothing unless it beat the stake, then the
    payout less the stake, or all of it when the stake is not returnable."""
    if payout <= amount:
        return 0
    return payout if returnable is False else payout - amount


def redeemable_of_bet(bet: dict[str, Any]) -> dict[str, Any] | None:
    """One bet from the bets subgraph as a claim: its on-chain id, whether it
    `won` or was `canceled`, the `amount` claiming pays the wallet, and the
    contracts to claim from. `None` for a bet with nothing to claim."""
    won = bet.get("result") == "Won"
    canceled = bet.get("status") == "Canceled"
    if not bet.get("isRedeemable") or bet.get("isCashedOut") or not (won or canceled):
        return None
    core = bet.get("core") or {}
    pool = core.get("liquidityPool") or {}
    decimals = int(pool.get("tokenDecimals") or 0)
    stake = int(D(bet.get("rawAmount")))
    raw = int(D(bet["rawPayout"])) if bet.get("rawPayout") not in (None, "") else \
        int(D(bet.get("rawPotentialPayout"))) if won else stake
    freebet = str(bet.get("freebetId") or "") or None
    if freebet:
        raw = freebet_share(raw, stake, bet.get("isFreebetAmountReturnable"))
    return {
        "bet_id": str(bet.get("tokenId") or bet.get("betId")),
        "combo": bet.get("type") == "Express",
        "result": "won" if won else "canceled",
        "amount": sig.token_amount(raw, decimals),
        "raw_amount": raw,
        "stake": sig.token_amount(stake, decimals),
        "core": str(core.get("address") or ""),
        "lp": str(pool.get("address") or ""),
        "freebet_id": freebet,
        "paymaster": bet.get("paymasterContractAddress"),
        "legs": [(str(((sel.get("outcome") or {}).get("condition") or {}).get("conditionId") or ""),
                  str((sel.get("outcome") or {}).get("outcomeId") or "")) for sel in bet.get("selections") or []],
        "info": bet,
    }


def claim_calls(bets: list[dict[str, Any]], *, chain: Chain, batch: int = CLAIM_BATCH) -> list[dict[str, Any]]:
    """The transactions that claim these bets (`redeemable_of_bet`), as the
    SDK's `useRedeemBet` builds them: one `withdrawPayouts(core, betIds)` to
    the pool's liquidity contract per `batch` bets of one core; one
    `withdrawPayout(core, betId)` each on a legacy pool; one paymaster
    `withdrawPayouts([freeBetId])` per free bet. Each is `{"to", "data",
    "bet_ids", "raw_amount"}`."""
    if batch < 1:
        raise InvalidOrder(f"{VENUE}: a claim batch holds at least one bet")
    calls: list[dict[str, Any]] = []
    pools: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for bet in bets:
        if bet.get("freebet_id"):
            if not bet.get("paymaster"):
                raise InvalidOrder(f"{VENUE}: free bet {bet['bet_id']} names no paymaster to claim from")
            calls.append({"to": bet["paymaster"], "data": sig.paymaster_withdraw_payouts_calldata([bet["freebet_id"]]),
                          "bet_ids": [bet["bet_id"]], "raw_amount": bet["raw_amount"]})
            continue
        pools.setdefault((bet["lp"].lower(), bet["core"].lower()), []).append(bet)
    for (lp, core), group in pools.items():
        if lp != chain.lp.lower():
            calls += [{"to": b["lp"], "data": sig.withdraw_payout_calldata(b["core"], b["bet_id"]),
                       "bet_ids": [b["bet_id"]], "raw_amount": b["raw_amount"]} for b in group]
            continue
        for start in range(0, len(group), batch):
            chunk = group[start:start + batch]
            calls.append({"to": chain.lp, "data": sig.withdraw_payouts_calldata(core, [b["bet_id"] for b in chunk]),
                          "bet_ids": [b["bet_id"] for b in chunk], "raw_amount": sum(b["raw_amount"] for b in chunk)})
    return calls


def claimable_positions_of(bets: list[dict[str, Any]], *, outcomes: dict[str, list[str]], decimals: int,
                           account: Account | None = None) -> list[Position]:
    """Settled single bets waiting to be claimed, per market: `resolved`,
    `won` (`None` when cancelled), and the amount waiting in `redeemable`.
    Combos have no one market and are left to `fetch_redeemable_bets`."""
    nets: dict[str, dict[str, Any]] = {}
    for bet in bets:
        if bet["combo"] or not bet["legs"]:
            continue
        condition_id, outcome_id = bet["legs"][0]
        market_id, side = market_of(condition_id, outcome_id, outcomes.get(condition_id))
        entry = nets.setdefault(market_id, {"yes": Decimal("0"), "no": Decimal("0"), "cost": Decimal("0"),
                                            "amount": Decimal("0"), "won": set(), "bets": []})
        info = bet["info"]
        entry["yes" if side == Side.BUY else "no"] += sig.token_amount(int(D(info.get("rawPotentialPayout"))), decimals)
        entry["cost"] += bet["stake"]
        entry["amount"] += bet["amount"]
        entry["won"].add(bet["result"] == "won")
        entry["bets"].append(bet["bet_id"])
    out = []
    for market_id, entry in nets.items():
        net = entry["yes"] - entry["no"]
        out.append(Position(
            venue=VENUE, account=account, market_id=market_id,
            side=PositionSide.LONG if net > 0 else PositionSide.SHORT if net < 0 else PositionSide.FLAT,
            contracts=abs(net), inventory_yes=entry["yes"], inventory_no=entry["no"], margin=entry["cost"],
            resolved=True, final=True, won=True if entry["won"] == {True} else None,
            payout=entry["amount"], redeemable=entry["amount"],
            info={"bets": "settled single bets not yet claimed", "bet_ids": entry["bets"]},
        ))
    return out


def error_of(exc: ExchangeError) -> ExchangeError:
    body = exc.body if isinstance(exc.body, dict) else {}
    text = str(body.get("errorMessage") or body.get("message") or body.get("error") or exc)
    lower = text.lower()
    if "balance" in lower or "allowance" in lower or "insufficient" in lower:
        return InsufficientFunds(f"{VENUE}: {text} -- the wallet needs the bet token, and an allowance to the "
                                 "relayer for the stake and its fee (approve())", body=exc.body)
    if any(word in lower for word in ("odds", "amount", "limit", "signature", "expired", "nonce")):
        return InvalidOrder(f"{VENUE}: {text}")
    return OrderRejected(f"{VENUE}: {text}", reason=lower or None, info=body, body=exc.body)


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

def check_request(request: OrderRequest) -> None:
    if request.type not in VENUE_ORDER_TYPES:
        raise InvalidOrder(
            f"{VENUE}: {request.type.value} is held by the execution engine, not the venue; submit it through the engine"
        )
    if request.type == OrderType.LIMIT and request.time_in_force not in (TimeInForce.IOC, TimeInForce.FOK):
        raise InvalidOrder(f"{VENUE}: the pool holds no resting orders; send a market order with a worst price, "
                           "or a limit that is ioc or fok")
    if request.time_in_force in (TimeInForce.GTD, TimeInForce.DAY):
        raise InvalidOrder(f"{VENUE}: a bet is taken at once or not at all; there is no {request.time_in_force.value}")
    if request.price is None:
        raise InvalidOrder(f"{VENUE}: a price is required -- a bet is sent with the worst price you give as its minimum odds")
    if request.post_only:
        raise InvalidOrder(f"{VENUE}: the pool only takes; there is no post-only bet")
    if request.reduce_only:
        raise InvalidOrder(f"{VENUE}: bets are not netted at the venue; there is no reduce-only bet")


def outcome_of(market_id: str, side: Side, outcomes: list[str]) -> tuple[str, str]:
    """(condition id, outcome id) a side of a market backs."""
    condition_id, outcome_id = parse_market_id(ids.native(VENUE, market_id))
    if outcome_id is not None:
        if len(outcomes) == 2:
            raise InvalidOrder(f"{VENUE}: {condition_id} has two outcomes and is one market, {VENUE}:{condition_id}")
        if outcomes and outcome_id not in outcomes:
            raise InvalidOrder(f"{VENUE}: condition {condition_id} has no outcome {outcome_id}")
        if side == Side.SELL:
            raise InvalidOrder(f"{VENUE}: one outcome of a larger condition has only YES to buy; the pool sells no NO")
        return condition_id, outcome_id
    if len(outcomes) != 2:
        raise InvalidOrder(f"{VENUE}: condition {condition_id} has {len(outcomes)} outcomes; name one: "
                           f"{', '.join(f'{VENUE}:{condition_id}-{o}' for o in outcomes)}")
    return condition_id, outcomes[0] if side == Side.BUY else outcomes[1]


def selection_of(request: OrderRequest, outcomes: list[str]) -> tuple[str, str, Decimal]:
    """(condition id, outcome id, price paid for the outcome) for a request."""
    condition_id, outcome_id = outcome_of(request.market_id, request.side, outcomes)
    price = D(request.price)
    return condition_id, outcome_id, price if request.side == Side.BUY else Decimal(1) - price


def build_bet(
    request: OrderRequest, *, chain: Chain, signer: WalletSigner, outcomes: list[str], relayer_fee: int,
    affiliate: str | None = None, nonce: int, now: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """`(body, bet)` for one request, signed, before anything is sent."""
    check_request(request)
    condition_id, outcome_id, paid = selection_of(request, outcomes)
    data = sig.client_data(chain, affiliate=affiliate or signer.address,
                           expires_at=int(time.time() if now is None else now) + DEADLINE_S, relayer_fee=relayer_fee)
    bet = sig.sub_bet(condition_id=condition_id, outcome_id=outcome_id, min_odds=sig.min_odds(paid),
                      amount=sig.stake(D(request.quantity), paid, chain.decimals), nonce=nonce)
    body = sig.wire_bet(chain, bettor=signer.address, data=data, bet=bet, signature=sig.sign_bet(signer, data, bet))
    return body, bet


def relayer_fee_of(info: Any) -> int:
    """The relayer's fee in base units from `/bet/gas-info`: an object in the
    toolkit, a one-element list in the API guide."""
    row = info[0] if isinstance(info, list) and info else info
    if not isinstance(row, dict) or row.get("relayerFeeAmount") in (None, ""):
        raise ExchangeError(f"{VENUE}: no relayer fee in /bet/gas-info: {info}", body=info)
    return int(D(row["relayerFeeAmount"]))


# ---------------------------------------------------------------------------
# Combos
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ComboLeg:
    """One leg of a combo: a market and the side backed there, as an order
    names them (`buy` its YES outcome, `sell` the other outcome of a
    two-outcome market)."""

    market_id: str
    side: Side = Side.BUY


def legs_of(legs: list[ComboLeg | str | tuple[str, Side | str]]) -> list[ComboLeg]:
    """Legs given as `ComboLeg`s, market ids (backing YES) or `(market_id, side)` pairs."""
    out = []
    for leg in legs:
        if isinstance(leg, ComboLeg):
            out.append(leg)
        elif isinstance(leg, str):
            out.append(ComboLeg(leg))
        else:
            market_id, side = leg
            out.append(ComboLeg(market_id, Side(side)))
    return out


@dataclass
class ComboQuote:
    """What the pool would take a combo at now: its total `odds` by the
    protocol's combo rule and their `price` (`1 / odds`, per contract), the
    pool's limits on it in the bet token, and every reason it would be
    refused (`problems`; none when `eligible`)."""

    legs: list[dict[str, Any]]
    odds: Decimal | None
    price: Decimal | None
    currency: str
    min_bet: Decimal | None = None
    max_bet: Decimal | None = None
    max_payout: Decimal | None = None
    problems: list[str] = field(default_factory=list)
    info: dict[str, Any] = field(default_factory=dict)

    @property
    def eligible(self) -> bool:
        return not self.problems


def _decimal(value: Any) -> Decimal | None:
    return D(value) if value not in (None, "") else None


def combo_quote_of(
    legs: list[ComboLeg], selections: list[tuple[str, str]], conditions: dict[str, dict[str, Any]],
    limits: dict[str, Any] | None, *, currency: str, stake: Decimal | None = None,
) -> ComboQuote:
    """A combo's quote from its legs' conditions (`Azuro.fetch_conditions`)
    and the pool's limits on it (`/bet/calculation`), with the checks the
    venue's SDK makes before it signs one: two or more legs, each condition
    once and each game once, no condition closed to combos
    (`isExpressForbidden`), every condition and outcome taking bets, a
    maximum stake above zero, and the stake within the limits."""
    problems: list[str] = []
    out_legs: list[dict[str, Any]] = []
    if len(selections) < MIN_COMBO_LEGS:
        problems.append(f"a combo needs at least {MIN_COMBO_LEGS} legs")
    seen_conditions: set[str] = set()
    seen_games: dict[str, str] = {}
    leg_odds: list[Decimal] = []
    for leg, (condition_id, outcome_id) in zip(legs, selections):
        condition = conditions.get(condition_id)
        game_id = game_of_condition(condition_id) or str(((condition or {}).get("game") or {}).get("gameId") or "") or None
        entry: dict[str, Any] = {"market_id": ids.qualify(VENUE, ids.native(VENUE, leg.market_id)), "side": leg.side,
                                 "condition_id": condition_id, "outcome_id": outcome_id, "game_id": game_id,
                                 "odds": None}
        out_legs.append(entry)
        if condition_id in seen_conditions:
            problems.append(f"condition {condition_id} is in the combo twice")
        seen_conditions.add(condition_id)
        if game_id is not None:
            if game_id in seen_games and seen_games[game_id] != condition_id:
                problems.append(f"game {game_id} is in the combo twice; legs must be on different games")
            seen_games.setdefault(game_id, condition_id)
        if condition is None:
            problems.append(f"no condition {condition_id}")
            continue
        if condition.get("isExpressForbidden"):
            problems.append(f"condition {condition_id} is not offered in combos")
        if str(condition.get("state") or "") != "Active":
            problems.append(f"condition {condition_id} is {condition.get('state') or 'not taking bets'}")
        outcome = next((o for o in condition.get("outcomes") or [] if str(o.get("outcomeId")) == outcome_id), None)
        if outcome is None:
            problems.append(f"condition {condition_id} has no outcome {outcome_id}")
            continue
        if str(outcome.get("state") or "Active") != "Active" or outcome.get("hidden") is True:
            problems.append(f"outcome {outcome_id} of {condition_id} is not taking bets")
        odds = odds_value(outcome_odds(outcome))
        entry["odds"] = odds
        if odds is None:
            problems.append(f"outcome {outcome_id} of {condition_id} has no odds")
        else:
            leg_odds.append(odds)
    total = sig.combo_odds(leg_odds) if leg_odds and len(leg_odds) == len(selections) else None
    if total is not None and total <= 1:
        problems.append(f"the combo's odds {total} are too low")
    raw = limits or {}
    min_bet, max_bet, max_payout = _decimal(raw.get("minBet")), _decimal(raw.get("maxBet")), _decimal(raw.get("maxPayout"))
    if limits is not None:
        if max_bet is not None and max_bet <= 0:
            problems.append("the pool takes nothing on this combo now")
        elif stake is not None and max_bet is not None and stake > max_bet:
            problems.append(f"a stake of {stake} is above the pool's maximum of {max_bet}")
        if stake is not None and min_bet is not None and stake < min_bet:
            problems.append(f"a stake of {stake} is below the pool's minimum of {min_bet}")
    return ComboQuote(
        legs=out_legs, odds=total, price=(Decimal(1) / total) if total else None, currency=currency,
        min_bet=min_bet, max_bet=max_bet, max_payout=max_payout, problems=problems,
        info={"limits": raw, "conditions": conditions},
    )


def build_combo(
    selections: list[tuple[str, str]], *, chain: Chain, signer: WalletSigner, quantity: Decimal, price: Decimal,
    relayer_fee: int, affiliate: str | None = None, nonce: int, now: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """`(body, combo)` for a combo, signed, before anything is sent: `quantity`
    contracts (what it pays if every leg wins) at a worst `price` per contract
    for the whole combo, which is minimum total odds `1 / price`."""
    if len(selections) < MIN_COMBO_LEGS:
        raise InvalidOrder(f"{VENUE}: a combo needs at least {MIN_COMBO_LEGS} legs")
    price = D(price)
    data = sig.client_data(chain, affiliate=affiliate or signer.address,
                           expires_at=int(time.time() if now is None else now) + DEADLINE_S, relayer_fee=relayer_fee)
    combo = sig.combo_bet(legs=selections, min_odds=sig.min_odds(price),
                          amount=sig.stake(D(quantity), price, chain.decimals), nonce=nonce)
    body = sig.wire_combo(chain, bettor=signer.address, data=data, combo=combo,
                          signature=sig.sign_combo(signer, data, combo))
    return body, combo


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class AzuroTrading(TradingExchange):
    """Azuro bets.

    ```python
    from synpath.trading.credentials import load_credentials, require
    from synpath.trading.azuro import AzuroTrading

    async with AzuroTrading(require("azuro", load_credentials())) as azuro:
        print(await azuro.fetch_balance(), await azuro.fetch_allowance())
    ```
    """

    id = VENUE
    name = "Azuro"
    has: dict[str, Capability] = {
        "create_order": True,
        # One request a bet; not atomic, one result each.
        "create_orders": True,
        # A bet on chain cannot be cancelled; it can be cashed out.
        "cancel_order": False,
        "cancel_orders": False,
        "cancel_all_orders": False,
        "edit_order": False,
        "fetch_order": True,
        # Bets the relayer has not yet put on chain.
        "fetch_open_orders": True,
        "fetch_orders": True,
        "fetch_my_trades": True,
        "fetch_positions": True,
        # The bet token in the wallet, read on chain.
        "fetch_balance": True,
        "fetch_settlements": True,
        "fetch_queue_position": False,
        # The relayer's flat fee per bet.
        "fetch_fee_estimate": True,
        "rfq": False,
        "split_merge": False,
        "watch_orders": False,
        "watch_my_trades": False,
        "watch_positions": False,
        "watch_balance": False,
    }

    def __init__(
        self,
        credentials: AzuroCredentials,
        *,
        account_name: str = "default",
        catalog: Any = None,
        limiter: BudgetLimiter | None = None,
        timeout: float = 30.0,
        client: Any = None,
    ):
        import httpx

        self.credentials = credentials
        self.chain = chain_of(credentials.chain)
        if self.chain.discontinued:
            raise BadRequest(f"{VENUE}: the {self.chain.name} deployment is discontinued and takes no bets")
        self.signer = WalletSigner(credentials.private_key)
        self.account = Account(venue=VENUE, name=account_name)
        self.limiter = limiter or BudgetLimiter(read_per_second=4, write_per_second=4)
        self.rpc_url = credentials.rpc_url or self.chain.rpc_url
        shared = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.http = AsyncHttpClient(self.chain.api_url, limiter=None, client=shared, venue=VENUE)
        self._own_catalog = catalog is None
        if catalog is None:
            catalog = Azuro(chain=self.chain)
        self.catalog = catalog
        """The read adapter: conditions' outcomes, which decide market ids and sides."""
        self._outcomes: dict[str, list[str]] = {}
        self._nonce = 0

    @property
    def address(self) -> str:
        return self.signer.address

    # -- plumbing -------------------------------------------------------------

    async def _call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, body: Any = None,
        kind: str = "read", priority: Priority = Priority.NORMAL,
    ) -> Any:
        await self.limiter.acquire(cost=1, kind=kind, priority=priority)  # type: ignore[arg-type]
        query = {k: v for k, v in (params or {}).items() if v is not None}
        return await self.http.request(method, path, params=query or None, json=body)

    async def _rpc(self, method: str, params: list[Any]) -> Any:
        raw = await self.http.request("POST", self.rpc_url, json={"jsonrpc": "2.0", "id": 1, "method": method,
                                                                  "params": params})
        if not isinstance(raw, dict) or raw.get("error") or "result" not in raw:
            raise ExchangeError(f"{VENUE}: {self.chain.name} rpc {method}: {raw}", body=raw)
        return raw["result"]

    async def outcomes_of(self, condition_ids: list[str]) -> dict[str, list[str]]:
        """Each condition's outcome ids, lowest first, read once and kept."""
        wanted = [c for c in dict.fromkeys(condition_ids) if c and c not in self._outcomes]
        if wanted:
            self._outcomes.update(await asyncio.to_thread(self.catalog.condition_outcomes, wanted))
        return {c: self._outcomes.get(c, []) for c in condition_ids}

    async def _orders_from(self, rows: list[dict[str, Any]]) -> dict[str, list[str]]:
        """The outcomes of every leg's condition in these rows."""
        return await self.outcomes_of([str(leg.get("conditionId") or "") for r in rows for leg in _legs(r)])

    def _next_nonce(self) -> int:
        """Milliseconds, as the venue's SDK uses, never repeated in this process."""
        self._nonce = max(self._nonce + 1, int(time.time() * 1000))
        return self._nonce

    async def relayer_fee(self) -> int:
        """The relayer's fee for one bet now, in the bet token's base units."""
        return relayer_fee_of(await self._call("GET", "/bet/gas-info", params={"environment": self.chain.environment}))

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        """Sign one bet and hand it to the relayer. Returns it pending; it is
        on chain once `fetch_order` says `closed` (accepted) or `rejected`."""
        condition_id, _ = parse_market_id(ids.native(VENUE, request.market_id))
        check_request(request)
        outcomes = (await self.outcomes_of([condition_id]))[condition_id]
        if not outcomes:
            raise MarketNotFound(f"{VENUE}: no condition {condition_id}")
        body, bet = build_bet(request, chain=self.chain, signer=self.signer, outcomes=outcomes,
                              relayer_fee=await self.relayer_fee(), affiliate=self.credentials.affiliate,
                              nonce=self._next_nonce())
        try:
            answer = await self._call("POST", "/bet/orders/ordinar", body=body, kind="write") or {}
        except (BadRequest, MarketNotFound) as exc:
            raise error_of(exc) from None
        state = str(answer.get("state") or "")
        if state != "Created" or not answer.get("id"):
            raise OrderRejected(f"{VENUE}: bet not created: {answer.get('errorMessage') or state or answer}",
                                reason=str(answer.get("error") or state or "rejected").lower(), info=answer)
        market_id, side = market_of(condition_id, bet["outcomeId"], outcomes)
        paid = Decimal(sig.ODDS_SCALE) / Decimal(int(bet["minOdds"]))
        quantity = sig.token_amount(bet["amount"], self.chain.decimals) / paid
        return Order(
            id=str(answer["id"]), client_order_id=request.client_order_id, venue=VENUE,
            account=request.account or self.account, market_id=market_id, side=side,
            type=request.type, time_in_force=request.time_in_force, status=OrderStatus.PENDING,
            price=paid if side == Side.BUY else Decimal(1) - paid,
            quantity=quantity.quantize(Decimal("0.000001")), filled=Decimal("0"),
            cost=sig.token_amount(bet["amount"], self.chain.decimals),
            created_at=int(time.time() * 1000), book=request.book, trader=request.trader, tags=request.tags,
            info={"answer": answer, "bet": bet},
        )

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many bets, one request each, one result each in the order asked for."""
        out: list[Order | Exception] = []
        for request in requests:
            try:
                out.append(await self.create_order(request))
            except (InvalidOrder, OrderRejected, InsufficientFunds, MarketNotFound, BadRequest) as exc:
                out.append(exc)
        return out

    async def wait_for_order(self, order_id: str, *, timeout_s: float = 30.0, poll_s: float = 1.0) -> Order:
        """Read the order until the relayer has put it on chain or it was
        refused, as the venue's SDK does; the last read when time runs out."""
        loop = asyncio.get_running_loop()
        stop = loop.time() + timeout_s
        while True:
            order = await self.fetch_order(order_id)
            if order.status != OrderStatus.PENDING or loop.time() >= stop:
                return order
            await asyncio.sleep(poll_s)

    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        raise NotSupported(f"{VENUE}: a bet cannot be cancelled once signed; cash it out with cash_out() where offered")

    async def fetch_order(self, order_id: str) -> Order:
        """One bet order by its id (`<bettor>_<nonce>`), in any state."""
        try:
            row = await self._call("GET", f"/bet/orders/{order_id}")
        except MarketNotFound:
            raise OrderNotFound(f"{VENUE}: no bet order {order_id}") from None
        if not isinstance(row, dict) or not row.get("id"):
            raise OrderNotFound(f"{VENUE}: no bet order {order_id}")
        outcomes = await self._orders_from([row])
        return order_of_row(row, outcomes=outcomes.get(str(_leg(row).get("conditionId") or "")), account=self.account,
                            legs_outcomes=outcomes)

    async def _bettor_rows(self, *, states: list[str] | None = None, result: list[str] | None = None,
                           offset: int = 0, limit: int | None = None) -> list[dict[str, Any]]:
        """This wallet's bet orders, newest first, page by page."""
        rows: list[dict[str, Any]] = []
        for _ in range(MAX_ORDER_PAGES):
            size = min(ORDERS_PAGE, (limit - len(rows)) if limit else ORDERS_PAGE)
            params: list[tuple[str, Any]] = [("environment", self.chain.environment), ("offset", offset), ("limit", size)]
            params += [("states", s) for s in states or []] + [("result", r) for r in result or []]
            try:
                raw = await self._bettor_page(params)
            except MarketNotFound:
                return rows
            page = [r for r in (raw or {}).get("orders") or [] if isinstance(r, dict)]
            rows += page
            offset += len(page)
            if len(page) < size or (limit and len(rows) >= limit):
                break
        return rows

    async def _bettor_page(self, params: list[tuple[str, Any]]) -> Any:
        await self.limiter.acquire(cost=1, kind="read", priority=Priority.NORMAL)  # type: ignore[arg-type]
        return await self.http.request("GET", f"/bet/orders/bettor/{self.address}", params=params)

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        """One page of this wallet's bets, newest first. `status` is `open`
        (not yet on chain) or `closed` (accepted, settled or ended); the cursor
        is the venue's offset into the list."""
        if cursor is not None and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        states = list(PENDING_STATES) if status == "open" else None
        offset, size = int(cursor or 0), min(limit or 50, ORDERS_PAGE)
        rows = await self._bettor_rows(states=states, offset=offset, limit=size)
        outcomes = await self._orders_from(rows)
        orders = [order_of_row(r, outcomes=outcomes.get(str(_leg(r).get("conditionId") or "")), account=self.account,
                               legs_outcomes=outcomes) for r in rows]
        if status == "closed":
            orders = [o for o in orders if o.is_terminal]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            orders = [o for o in orders if o.market_id == wanted]
        if since:
            orders = [o for o in orders if (o.created_at or 0) >= since]
        return Page(orders, next_cursor=str(offset + len(rows)) if len(rows) >= size else None)

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Bets signed and not yet on chain: nothing rests in a pool."""
        rows = await self._bettor_rows(states=list(PENDING_STATES))
        outcomes = await self._orders_from(rows)
        orders = [order_of_row(r, outcomes=outcomes.get(str(_leg(r).get("conditionId") or "")), account=self.account,
                               legs_outcomes=outcomes) for r in rows]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            orders = [o for o in orders if o.market_id == wanted]
        return orders

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        """This wallet's accepted single bets, one fill each, newest first."""
        if cursor is not None and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        offset, size = int(cursor or 0), min(limit or 50, ORDERS_PAGE)
        rows = await self._bettor_rows(states=list(FILLED_STATES), offset=offset, limit=size)
        outcomes = await self._orders_from(rows)
        fills = [f for f in (fill_of_row(r, outcomes=outcomes.get(str(_leg(r).get("conditionId") or "")),
                                         account=self.account) for r in rows) if f is not None]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            fills = [f for f in fills if f.market_id == wanted]
        if order_id:
            fills = [f for f in fills if f.order_id == order_id]
        if since:
            fills = [f for f in fills if f.timestamp >= since]
        return Page(fills, next_cursor=str(offset + len(rows)) if len(rows) >= size else None)

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        """This wallet's settled bets, singles and combos: won, lost or
        cancelled, each with whether its payout is claimed (`info["claimed"]`,
        `info["claimable"]`)."""
        if cursor is not None and not cursor.isdigit():
            raise BadRequest(f"{VENUE}: {cursor!r} is not a cursor this API issued")
        offset, size = int(cursor or 0), min(limit or 50, ORDERS_PAGE)
        rows = await self._bettor_rows(result=["Won", "Lost", "Canceled"], offset=offset, limit=size)
        outcomes = await self._orders_from(rows)
        out = [s for s in (settlement_of_row(r, outcomes=outcomes.get(str(_leg(r).get("conditionId") or "")),
                                             account=self.account, legs_outcomes=outcomes) for r in rows)
               if s is not None]
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            out = [s for s in out if s.market_id == wanted]
        if since:
            out = [s for s in out if (s.timestamp or 0) >= since]
        return Page(out, next_cursor=str(offset + len(rows)) if len(rows) >= size else None)

    # -- account --------------------------------------------------------------

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """The wallet's bet token, read on chain. Accepted bets are held by
        the pool, not the wallet, so nothing here is `locked`."""
        raw = await self._rpc("eth_call", [{"to": self.chain.token, "data": sig.balance_calldata(self.address)}, "latest"])
        total = sig.token_amount(int(raw, 16), self.chain.decimals)
        return Balance(
            venue=VENUE, account=account or self.account, currency=self.chain.symbol,
            total=total, available=total, locked=None, buying_power=None,
            timestamp=int(time.time() * 1000), info={"address": self.address, "token": self.chain.token,
                                                     "chain": self.chain.name},
        )

    async def fetch_allowance(self) -> Decimal:
        """What the relayer may spend of the wallet's bet token, read on chain."""
        raw = await self._rpc("eth_call", [{"to": self.chain.token, "data": sig.allowance_calldata(
            self.address, self.chain.relayer)}, "latest"])
        return sig.token_amount(int(raw, 16), self.chain.decimals)

    async def approve(self, amount: Decimal | None = None, *, wait: bool = True, timeout_s: float = 120.0) -> dict[str, Any]:
        """Allow the relayer to spend the wallet's bet token: unlimited by
        default, as the venue's SDK does, or `amount` tokens. An on-chain
        transaction from the wallet, which pays its gas in the chain's coin."""
        value = MAX_UINT256 if amount is None else int(D(amount) * (Decimal(10) ** self.chain.decimals))
        call = {"to": self.chain.token, "data": sig.approve_calldata(self.chain.relayer, value)}
        estimate = await self._estimate(call, what="approval")
        [sent] = await self._send_calls([(call, estimate)])
        if not wait:
            return {"transaction_hash": sent}
        return {"transaction_hash": sent, "receipt": await self._receipt(sent, timeout_s=timeout_s, what="approval")}

    # -- wallet transactions --------------------------------------------------

    async def _estimate(self, call: dict[str, Any], *, what: str) -> int:
        """The gas a call from the wallet takes; a call that would revert is
        refused here, named by the contract's error where it is one of the
        pool's, before anything is sent."""
        try:
            return int(await self._rpc("eth_estimateGas", [{"from": self.address, "to": call["to"],
                                                            "data": call["data"]}]), 16)
        except ExchangeError as exc:
            error = (exc.body or {}).get("error") if isinstance(exc.body, dict) else None
            data = str((error or {}).get("data") or "") if isinstance(error, dict) else ""
            name = sig.PAYOUT_ERRORS.get(data[:10].lower())
            reason = name or (str(error.get("message")) if isinstance(error, dict) else "reverted")
            raise OrderRejected(f"{VENUE}: {what} would revert: {reason}", reason=(name or "reverted").lower(),
                                info={"call": call, "error": error}) from None

    async def _send_calls(self, calls: list[tuple[dict[str, Any], int]]) -> list[str]:
        """Sign and send calls from the wallet, one transaction each with
        consecutive nonces; their hashes. The wallet pays the gas."""
        nonce = int(await self._rpc("eth_getTransactionCount", [self.address, "pending"]), 16)
        gas_price = int(await self._rpc("eth_gasPrice", []), 16)
        priority_fee = int(await self._rpc("eth_maxPriorityFeePerGas", []), 16)
        hashes = []
        for offset, (call, estimate) in enumerate(calls):
            tx = {
                "chainId": self.chain.chain_id, "nonce": nonce + offset, "to": call["to"], "value": 0,
                "data": call["data"], "gas": int(estimate * 1.2), "maxFeePerGas": gas_price * 2 + priority_fee,
                "maxPriorityFeePerGas": priority_fee, "type": 2,
            }
            hashes.append(await self._rpc("eth_sendRawTransaction", [self.signer.sign_transaction(tx)]))
        return hashes

    async def _receipt(self, tx_hash: str, *, timeout_s: float, what: str, poll_s: float = 2.0) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        stop = loop.time() + timeout_s
        while True:
            receipt = await self._rpc("eth_getTransactionReceipt", [tx_hash])
            if receipt:
                if int(receipt.get("status", "0x0"), 16) != 1:
                    raise OrderRejected(f"{VENUE}: {what} {tx_hash} reverted", reason="reverted", info=receipt)
                return receipt
            if loop.time() >= stop:
                raise ExchangeError(f"{VENUE}: {what} {tx_hash} unmined after {timeout_s}s; it may yet confirm")
            await asyncio.sleep(poll_s)

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Accepted, unsettled single bets netted per market on the YES leg,
        and settled single bets whose payout waits to be claimed (`resolved`,
        the amount in `redeemable`)."""
        rows = await self._bettor_rows(states=["Accepted", "CancelFailed"])
        rows = [r for r in rows if r.get("result") in (None, "")]
        claims = await self.fetch_redeemable_bets()
        outcomes = await self.outcomes_of([str(leg.get("conditionId") or "") for r in rows for leg in _legs(r)]
                                          + [b["legs"][0][0] for b in claims if not b["combo"] and b["legs"]])
        positions = positions_of(rows, outcomes=outcomes, account=self.account)
        positions += claimable_positions_of(claims, outcomes=outcomes, decimals=self.chain.decimals, account=self.account)
        if market_id:
            wanted = ids.qualify(VENUE, ids.native(VENUE, market_id))
            positions = [p for p in positions if p.market_id == wanted]
        if event_id:
            game = ids.native(VENUE, event_id)
            positions = [p for p in positions
                         if game_of_condition(parse_market_id(ids.split(p.market_id)[1])[0]) == game]
        return positions

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """The relayer's flat fee for one bet, in the bet token: there is no
        fee on top of the pool's odds, whose margin is already in them."""
        fee = sig.token_amount(await self.relayer_fee(), self.chain.decimals)
        return FeeEstimate(
            venue=VENUE, market_id=ids.qualify(VENUE, ids.native(VENUE, market_id)), side=side,
            price=D(price), quantity=D(quantity), taker_fee=fee, maker_fee=None,
            currency=self.chain.symbol, info={"charged": "per bet, to the relayer, for the transaction"},
        )

    # -- combos ---------------------------------------------------------------

    async def _combo_selections(self, legs: list[ComboLeg]) -> list[tuple[str, str]]:
        conditions = [parse_market_id(ids.native(VENUE, leg.market_id))[0] for leg in legs]
        outcomes = await self.outcomes_of(conditions)
        for condition_id in conditions:
            if not outcomes[condition_id]:
                raise MarketNotFound(f"{VENUE}: no condition {condition_id}")
        return [outcome_of(leg.market_id, leg.side, outcomes[c]) for leg, c in zip(legs, conditions)]

    async def fetch_combo_limits(self, selections: list[tuple[str, str]]) -> dict[str, Any]:
        """The pool's limits on these legs together (`/bet/calculation`), for
        this wallet: `minBet`, `maxBet` and `maxPayout` in the bet token."""
        raw = await self._call("POST", "/bet/calculation", body={
            "bets": [{"conditionId": c, "outcomeId": int(o)} for c, o in selections],
            "environment": self.chain.environment, "wallet": self.address,
        })
        raw = (raw or {}).get("response", raw) if isinstance(raw, dict) else {}
        return raw if isinstance(raw, dict) else {}

    async def fetch_combo_quote(
        self, legs: list[ComboLeg | str | tuple[str, Side | str]], *, stake: Decimal | None = None,
    ) -> ComboQuote:
        """What the pool would take a combo of these legs at now: the total
        odds by the protocol's combo rule from the legs' current odds, their
        price, the pool's limits on the combo, and why it would be refused
        (`problems`). `stake` (in the bet token) is checked against the
        limits. Nothing is signed or sent."""
        legs = legs_of(legs)
        selections = await self._combo_selections(legs)
        conditions = await asyncio.to_thread(self.catalog.fetch_conditions, [c for c, _ in selections])
        limits: dict[str, Any] | None = None
        refused = None
        try:
            limits = await self.fetch_combo_limits(selections)
        except (BadRequest, MarketNotFound) as exc:
            refused = str(exc)
        quote = combo_quote_of(legs, selections, conditions, limits, currency=self.chain.symbol,
                               stake=D(stake) if stake is not None else None)
        if refused:
            quote.problems.append(f"the pool's limits were refused: {refused}")
        return quote

    async def create_combo_order(
        self, legs: list[ComboLeg | str | tuple[str, Side | str]], *, quantity: Decimal, price: Decimal,
        client_order_id: str | None = None, check: bool = True,
    ) -> Order:
        """Sign one combo and hand it to the relayer (`/bet/orders/combo`):
        `quantity` contracts, what it pays if every leg wins, at a worst
        `price` per contract for the whole combo, so its minimum total odds
        are `1 / price` and its stake `quantity * price`. A market order with
        a worst price, like a single bet: the pool takes it whole at its odds
        when it reaches the chain, or refuses it. With `check` (the default)
        the quote's problems are raised before anything is signed. Returns the
        order pending, on its first leg's market, marked `info["combo"]`;
        follow it with `fetch_order` or `wait_for_order`."""
        legs = legs_of(legs)
        price, quantity = D(price), D(quantity)
        selections = await self._combo_selections(legs)
        if check:
            quote = await self.fetch_combo_quote(legs, stake=quantity * price)
            if quote.problems:
                raise InvalidOrder(f"{VENUE}: combo refused before signing: {'; '.join(quote.problems)}")
        body, combo = build_combo(selections, chain=self.chain, signer=self.signer, quantity=quantity, price=price,
                                  relayer_fee=await self.relayer_fee(), affiliate=self.credentials.affiliate,
                                  nonce=self._next_nonce())
        try:
            answer = await self._call("POST", "/bet/orders/combo", body=body, kind="write") or {}
        except (BadRequest, MarketNotFound) as exc:
            raise error_of(exc) from None
        state = str(answer.get("state") or "")
        if state != "Created" or not answer.get("id"):
            raise OrderRejected(f"{VENUE}: combo not created: {answer.get('errorMessage') or state or answer}",
                                reason=str(answer.get("error") or state or "rejected").lower(), info=answer)
        outcomes = await self.outcomes_of([c for c, _ in selections])
        named = [dict(zip(("market_id", "side"), market_of(c, o, outcomes[c])), condition_id=c, outcome_id=o)
                 for c, o in selections]
        paid = Decimal(sig.ODDS_SCALE) / Decimal(int(combo["minOdds"]))
        cost = sig.token_amount(combo["amount"], self.chain.decimals)
        return Order(
            id=str(answer["id"]), client_order_id=client_order_id, venue=VENUE, account=self.account,
            market_id=named[0]["market_id"], side=Side.BUY, type=OrderType.MARKET, time_in_force=TimeInForce.FOK,
            status=OrderStatus.PENDING, price=paid, quantity=(cost / paid).quantize(Decimal("0.000001")),
            filled=Decimal("0"), cost=cost, created_at=int(time.time() * 1000),
            info={"answer": answer, "combo": True, "bet": combo, "legs": named},
        )

    # -- claims ---------------------------------------------------------------

    async def _redeemable_page(self, skip: int) -> list[dict[str, Any]]:
        await self.limiter.acquire(cost=1, kind="read", priority=Priority.NORMAL)  # type: ignore[arg-type]
        raw = await self.http.request("POST", self.chain.subgraph_url, json={
            "query": REDEEMABLE_QUERY,
            "variables": {"first": SUBGRAPH_PAGE, "skip": skip,
                          "where": {"actor": self.address.lower(), "isRedeemable": True, "isCashedOut": False}},
        })
        if not isinstance(raw, dict) or raw.get("errors") or not isinstance(raw.get("data"), dict):
            raise ExchangeError(f"{VENUE}: bets subgraph: {raw}", body=raw)
        return [b for b in raw["data"].get("v3Bets") or [] if isinstance(b, dict)]

    async def fetch_redeemable_bets(self) -> list[dict[str, Any]]:
        """This wallet's bets whose payout is still in the pool, from the bets
        subgraph: won, or cancelled (the stake returned), not cashed out, not
        yet claimed. Each is `{"bet_id", "combo", "result" ("won" or
        "canceled"), "amount" (what claiming pays, in the bet token), "stake",
        "core", "lp", "freebet_id", "paymaster", "legs", "info"}`."""
        bets: list[dict[str, Any]] = []
        for page in range(MAX_SUBGRAPH_PAGES):
            rows = await self._redeemable_page(page * SUBGRAPH_PAGE)
            bets += [b for b in (redeemable_of_bet(r) for r in rows) if b is not None]
            if len(rows) < SUBGRAPH_PAGE:
                break
        return bets

    async def fetch_claimable(self, bet_id: str | int, *, core: str | None = None) -> Decimal:
        """What claiming one bet would pay now, read on chain from the pool
        (`viewPayout`): zero once claimed, for a lost bet, or before it settles."""
        raw = await self._rpc("eth_call", [{"to": self.chain.lp, "data": sig.view_payout_calldata(
            core or self.chain.core, bet_id)}, "latest"])
        return sig.token_amount(int(raw, 16), self.chain.decimals)

    async def redeem(
        self, bet_ids: list[str | int] | None = None, *, wait: bool = True, timeout_s: float = 120.0,
        batch: int = CLAIM_BATCH,
    ) -> dict[str, Any]:
        """Claim won and cancelled bets' payouts into the wallet: every bet
        `fetch_redeemable_bets` lists, or those of `bet_ids`. One
        `withdrawPayouts` transaction to the pool per `batch` bets, one per
        free bet (the paymaster's), sent from the wallet, which pays the gas in
        the chain's coin. Each call is first estimated, so one that would
        revert (already claimed: `alreadypaid`) is refused before anything is
        sent. Returns `{"transactions": [{"transaction_hash", "to",
        "bet_ids", "amount", "receipt"}], "bet_ids", "amount", "currency"}`;
        nothing to claim sends nothing."""
        bets = await self.fetch_redeemable_bets()
        if bet_ids is not None:
            wanted = [str(b) for b in bet_ids]
            known = {b["bet_id"] for b in bets}
            missing = [b for b in wanted if b not in known]
            if missing:
                raise InvalidOrder(f"{VENUE}: nothing to claim on bet {', '.join(missing)}: not won or cancelled, "
                                   "cashed out, already claimed, or not this wallet's")
            bets = [b for b in bets if b["bet_id"] in wanted]
        calls = claim_calls(bets, chain=self.chain, batch=batch)
        if not calls:
            return {"transactions": [], "bet_ids": [], "amount": Decimal("0"), "currency": self.chain.symbol}
        estimates = [await self._estimate(call, what=f"claim of bet {', '.join(call['bet_ids'])}") for call in calls]
        hashes = await self._send_calls(list(zip(calls, estimates)))
        out = []
        for call, tx_hash in zip(calls, hashes):
            entry: dict[str, Any] = {"transaction_hash": tx_hash, "to": call["to"], "bet_ids": call["bet_ids"],
                                     "amount": sig.token_amount(call["raw_amount"], self.chain.decimals)}
            if wait:
                entry["receipt"] = await self._receipt(tx_hash, timeout_s=timeout_s, what="claim")
            out.append(entry)
        return {"transactions": out, "bet_ids": [b for call in calls for b in call["bet_ids"]],
                "amount": sum((e["amount"] for e in out), Decimal("0")), "currency": self.chain.symbol}

    claim_winnings = redeem

    # -- cash-out -------------------------------------------------------------

    async def fetch_cashout_quote(self, bet_id: str | int) -> dict[str, Any] | None:
        """What the venue offers now to close one accepted bet early
        (`/cashout/get-calculation`), by the bet's on-chain id (`Order.info["betId"]`);
        `None` when it offers nothing."""
        try:
            raw = await self._call("POST", "/cashout/get-calculation", body={
                "environment": self.chain.environment, "owner": self.address, "betId": str(bet_id),
            })
        except MarketNotFound:
            return None
        return raw if isinstance(raw, dict) and raw.get("calculationId") else None

    async def cash_out(self, bet_id: str | int, *, quote: dict[str, Any] | None = None) -> dict[str, Any]:
        """Close one accepted bet at the venue's cash-out offer: the offer is
        read (or given), signed by the wallet with its odds as the minimum,
        and sent (`/cashout/create`). Returns the cash-out order; read it on
        with `fetch_cashout`."""
        quote = quote or await self.fetch_cashout_quote(bet_id)
        if not quote:
            raise OrderRejected(f"{VENUE}: no cash-out offered on bet {bet_id}", reason="no_cashout")
        message = sig.cashout_message(self.chain, bet_id=bet_id, cashout_odds=str(quote["cashoutOdds"]),
                                      expires_at=int(quote["expiredAt"]))
        body = sig.wire_cashout(self.chain, calculation_id=str(quote["calculationId"]), attention=message["attention"],
                                signature=sig.sign_cashout(self.signer, self.chain, message))
        try:
            answer = await self._call("POST", "/cashout/create", body=body, kind="write") or {}
        except (BadRequest, MarketNotFound) as exc:
            raise error_of(exc) from None
        if str(answer.get("state") or "").upper() == "REJECTED":
            raise OrderRejected(f"{VENUE}: cash-out refused: {answer.get('errorMessage') or answer}",
                                reason="cashout_rejected", info=answer)
        return answer

    async def fetch_cashout(self, cashout_id: str) -> dict[str, Any]:
        """One cash-out order: its state and, once done, its transaction."""
        try:
            return await self._call("GET", f"/cashout/{cashout_id}") or {}
        except MarketNotFound:
            raise OrderNotFound(f"{VENUE}: no cash-out {cashout_id}") from None

    async def close(self) -> None:
        await self.http.close()
        if self._own_catalog:
            self.catalog.close()


__all__ = [
    "AzuroTrading", "ComboLeg", "ComboQuote", "order_of_row", "fill_of_row", "settlement_of_row", "positions_of",
    "claimable_positions_of", "build_bet", "build_combo", "combo_quote_of", "redeemable_of_bet", "claim_calls",
]
