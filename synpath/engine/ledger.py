"""The ledger: what the fills add up to.

A venue tells you your position. It does not tell you what each strategy
paid for it, what is realized, or what the same exposure looks like across
three venues. That is this file's job, and it works only from fills the
journal already holds, so it can be rebuilt from the log at any time.

Three decisions shape it:

**Everything nets on the YES leg.** Buying NO at 0.40 is selling YES at
0.60, and a book holding both is holding neither. Fills on a `:no`
instrument are converted before they are applied, so a position is one
number per market instead of two that quietly cancel. What was actually
bought is kept as well, per instrument, because on Polymarket those are real
tokens that must be redeemed or merged.

**Average cost, and realized profit taken on the way out.** A reducing fill
realizes against the average cost of what is being closed; a fill that
crosses through zero closes the old side first and opens the new one at the
fill price. Fees are realized when they are charged, never amortized into
cost, so a flat book's realized profit is what the venue actually paid.

**Unrealized profit needs a mark, and a mark is a choice.** It comes from
fair values the engine stores per account and instrument (`fair_values.py`),
not from a book this module cannot see. Without a mark a position reports
its cost and no unrealized number, rather than a made-up one.

Settlement closes a position at 1 or 0 and moves the whole remaining amount
into realized.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Literal

from ..trading.types import Account, Fill, Position, PositionSide, Settlement, SettlementState, Side

ZERO = Decimal("0")
ONE = Decimal("1")

Level = Literal["market", "book", "account"]


@dataclass(slots=True)
class PositionState:
    """One book's position in one market, netted on the YES leg."""

    account_key: str
    book: str
    market_id: str
    venue: str
    contracts: Decimal = ZERO
    """Signed: positive is long YES, negative is short YES (long NO)."""
    average_cost: Decimal = ZERO
    """Per contract, of what is currently open; for a short, what was received."""
    realized: Decimal = ZERO
    fees: Decimal = ZERO
    bought: Decimal = ZERO
    sold: Decimal = ZERO
    volume: Decimal = ZERO
    """Contracts traded, both directions."""
    inventory: dict[str, Decimal] = field(default_factory=dict)
    """Contracts held per side (`"yes"`, `"no"`) before netting, as the
    engine's own fills imply: a sell adds to `no`."""
    resolved: bool = False
    last_ts: int | None = None

    def average_price_in_bucket(self, flip: bool, face_value: Decimal = ONE) -> Decimal:
        """The average cost as a price of the bucket's YES: unchanged for a
        member that is the bucket's YES, reflected through the face value
        for one that is its NO."""
        return face_value - self.average_cost if flip else self.average_cost
    fills: int = 0

    @property
    def side(self) -> PositionSide:
        if self.contracts > 0:
            return PositionSide.LONG
        if self.contracts < 0:
            return PositionSide.SHORT
        return PositionSide.FLAT

    @property
    def cost(self) -> Decimal:
        """What the open position tied up, positive either way."""
        return abs(self.contracts) * self.average_cost

    def unrealized(self, mark: Decimal | None) -> Decimal | None:
        """Profit if the position were closed at `mark`. `None` without a mark."""
        if mark is None or self.contracts == 0:
            return None
        if self.contracts > 0:
            return (mark - self.average_cost) * self.contracts
        # Short YES: sold at average_cost, bought back at mark.
        return (self.average_cost - mark) * abs(self.contracts)

    def net(self, mark: Decimal | None = None) -> Decimal:
        unreal = self.unrealized(mark) or ZERO
        return self.realized + unreal

    def apply(self, *, side: Side, price: Decimal, amount: Decimal, fee: Decimal | None, ts: int | None) -> Decimal:
        """Apply one fill already converted to the YES leg. Returns the profit
        realized by this fill, fee included."""
        signed = amount if side == Side.BUY else -amount
        realized = ZERO
        if self.contracts == 0 or (self.contracts > 0) == (signed > 0):
            # Opening or adding: weighted average of what it cost.
            total = abs(self.contracts) + amount
            self.average_cost = ((self.average_cost * abs(self.contracts)) + (price * amount)) / total if total else ZERO
            self.contracts += signed
        else:
            closing = min(amount, abs(self.contracts))
            direction = ONE if self.contracts > 0 else -ONE
            realized += (price - self.average_cost) * closing * direction
            self.contracts += signed
            if self.contracts == 0:
                self.average_cost = ZERO
            elif (self.contracts > 0) != (direction > 0):
                # Crossed through zero: the remainder opens the other side.
                self.average_cost = price
        if fee:
            self.fees += fee
            realized -= fee
        self.realized += realized
        self.volume += amount
        if side == Side.BUY:
            self.bought += amount
        else:
            self.sold += amount
        held = "yes" if side == Side.BUY else "no"
        self.inventory[held] = self.inventory.get(held, ZERO) + amount
        self.fills += 1
        if ts is not None:
            self.last_ts = max(ts, self.last_ts or 0)
        return realized

    def settle(self, price: Decimal) -> Decimal:
        """Resolution: everything still open pays out at `price` (1 or 0)."""
        if self.contracts == 0:
            self.resolved = True
            return ZERO
        realized = (price - self.average_cost) * self.contracts
        self.realized += realized
        self.contracts = ZERO
        self.average_cost = ZERO
        self.resolved = True
        return realized

    def to_position(self, account: Account | None = None, mark: Decimal | None = None) -> Position:
        """The unified `Position` a caller sees."""
        return Position(
            venue=self.venue, account=account, market_id=self.market_id,
            side=self.side, contracts=abs(self.contracts),
            inventory_yes=self.inventory.get("yes"), inventory_no=self.inventory.get("no"),
            entry_price=self.average_cost if self.contracts else None, mark_price=mark,
            unrealized_pnl=self.unrealized(mark), realized_pnl=self.realized, resolved=self.resolved,
            timestamp=self.last_ts,
            info={"book": self.book, "fees": str(self.fees), "volume": str(self.volume), "fills": self.fills},
        )


@dataclass(slots=True)
class Rollup:
    """A level of the tree: one instrument, market, book or account."""

    level: Level
    key: str
    contracts: Decimal = ZERO
    """Net signed contracts; for a roll-up above one market it is the sum,
    which nets long one market against short another only if they are the
    same market."""
    cost: Decimal = ZERO
    realized: Decimal = ZERO
    unrealized: Decimal | None = None
    fees: Decimal = ZERO
    volume: Decimal = ZERO
    positions: int = 0
    marked: int = 0
    """How many of the open positions had a mark; the rest are cost only."""

    @property
    def total(self) -> Decimal:
        return self.realized + (self.unrealized or ZERO)


class Ledger:
    """Positions and profit, per account, book and market.

    Built by replaying fills in order. A fill already applied is ignored, so
    replaying the journal twice cannot double a position.
    """

    def __init__(self, *, face_value: Decimal = ONE):
        self.face_value = face_value
        self.positions: dict[tuple[str, str, str], PositionState] = {}
        self.seen: set[tuple[str, str]] = set()
        self.settled: set[tuple[str, str, str]] = set()

    # -- applying -------------------------------------------------------------

    def apply_fill(self, fill: Fill, *, book: str | None = None) -> Decimal:
        """Book one fill. Returns the realized profit it produced."""
        if (fill.venue, fill.id) in self.seen:
            return ZERO
        self.seen.add((fill.venue, fill.id))
        if fill.settlement == SettlementState.FAILED:
            # Matched and then lost on chain: it never happened.
            return ZERO
        state = self.state_for(fill, book=book)
        # Fills arrive on the YES leg already: `sell` is the NO side at the YES price.
        return state.apply(side=fill.side, price=fill.price, amount=fill.amount, fee=fill.fee, ts=fill.timestamp)

    def apply_fills(self, fills: Iterable[Fill], *, book: str | None = None) -> Decimal:
        return sum((self.apply_fill(f, book=book) for f in fills), ZERO)

    def apply_settlement(self, settlement: Settlement, *, book: str | None = None) -> Decimal:
        """A market resolved. Closes every book's position in it at the payout."""
        price = _settlement_price(settlement, self.face_value)
        realized = ZERO
        for key, state in self.positions.items():
            if state.venue == settlement.venue and state.market_id == settlement.market_id:
                if key in self.settled:
                    continue
                if book is not None and state.book != book:
                    continue
                realized += state.settle(price)
                self.settled.add(key)
        return realized

    def state_for(self, fill: Fill, *, book: str | None = None) -> PositionState:
        account_key = fill.account.key if fill.account else f"{fill.venue}:default"
        name = book or (fill.info.get("book") if isinstance(fill.info, dict) else None) or "default"
        key = (account_key, name, fill.market_id)
        state = self.positions.get(key)
        if state is None:
            state = self.positions[key] = PositionState(
                account_key=account_key, book=name, market_id=fill.market_id, venue=fill.venue,
            )
        return state

    # -- reading --------------------------------------------------------------

    def open_positions(self) -> list[PositionState]:
        return [p for p in self.positions.values() if p.contracts != 0]

    def position(self, account_key: str, book: str, venue: str, market_id: str) -> PositionState | None:
        return self.positions.get((account_key, book, market_id))

    def rollup(self, level: Level, marks: dict[tuple[str, str], Decimal] | None = None) -> dict[str, Rollup]:
        """Aggregate every position to one of the four levels.

        `marks` is keyed by `(account_key, market_id)`, as the journal
        stores fair values.
        """
        out: dict[str, Rollup] = {}
        for state in self.positions.values():
            if level in ("market", "instrument"):
                key = state.market_id
            elif level == "market":
                key = f"{state.venue}:{state.market_id}"
            elif level == "book":
                key = state.book
            else:
                key = state.account_key
            row = out.get(key)
            if row is None:
                row = out[key] = Rollup(level=level, key=key)
            mark = (marks or {}).get((state.account_key, state.market_id))
            unreal = state.unrealized(mark)
            row.contracts += state.contracts
            row.cost += state.cost
            row.realized += state.realized
            row.fees += state.fees
            row.volume += state.volume
            row.positions += 1 if state.contracts else 0
            if unreal is not None:
                row.unrealized = (row.unrealized or ZERO) + unreal
                row.marked += 1
        return out

    def total(self, marks: dict[tuple[str, str], Decimal] | None = None) -> Rollup:
        """The firm's line: everything, every account, every book."""
        row = Rollup(level="account", key="*")
        for part in self.rollup("account", marks).values():
            row.contracts += part.contracts
            row.cost += part.cost
            row.realized += part.realized
            row.fees += part.fees
            row.volume += part.volume
            row.positions += part.positions
            row.marked += part.marked
            if part.unrealized is not None:
                row.unrealized = (row.unrealized or ZERO) + part.unrealized
        return row

    def merged(self, venue_positions: Iterable[Position]) -> list[dict[str, Any]]:
        """The ledger and the venue side by side, read only.

        One row per account and market: what this engine's books add up to,
        what the venue says, and the difference. A non-zero difference is a
        position somebody else in the account opened, or a fill this engine
        has not seen; `reconcile.py` turns it into an event.
        """
        mine: dict[tuple[str, str], Decimal] = {}
        books: dict[tuple[str, str], dict[str, Decimal]] = {}
        for state in self.positions.values():
            key = (state.account_key, state.market_id)
            mine[key] = mine.get(key, ZERO) + state.contracts
            books.setdefault(key, {})[state.book] = state.contracts
        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for position in venue_positions:
            account_key = position.account.key if position.account else f"{position.venue}:default"
            signed = position.contracts if position.side != PositionSide.SHORT else -position.contracts
            key = (account_key, position.market_id)
            seen.add(key)
            engine = mine.get(key, ZERO)
            rows.append({
                "account": account_key, "venue": position.venue,
                "market_id": position.market_id, "engine": engine, "venue_contracts": signed,
                "difference": engine - signed, "books": books.get(key, {}),
            })
        for key, engine in mine.items():
            if key in seen or engine == 0:
                continue
            account_key, market_id = key
            venue, _, _ = market_id.partition(":")
            rows.append({
                "account": account_key, "venue": venue,
                "market_id": market_id, "engine": engine, "venue_contracts": ZERO,
                "difference": engine, "books": books.get(key, {}),
            })
        return sorted(rows, key=lambda r: (r["account"], r["venue"], r["market_id"]))


def _settlement_price(settlement: Settlement, face_value: Decimal) -> Decimal:
    """What one YES contract paid at resolution.

    The venues say this three different ways, so take them in order of how
    directly they answer the question: the outcome word, then the flag, then
    the payout divided by what it was paid on.
    """
    result = (settlement.result or "").strip().lower()
    if result in ("yes", "true", "1"):
        return face_value
    if result in ("no", "false", "0"):
        return ZERO
    if settlement.won is not None:
        return face_value if settlement.won else ZERO
    if settlement.payout is not None and settlement.amount:
        return Decimal(settlement.payout) / Decimal(settlement.amount)
    return ZERO
