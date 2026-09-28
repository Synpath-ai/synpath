"""Order entry through your own `synpath serve`, with the `Client` method names.

`synpath.Client(server="http://127.0.0.1:8000")` sends every trading call to
that server's `/trading` routes instead of to the venues, which is what
makes the engine's order types reachable from Python: a stop, an iceberg,
an order on a bucket. Market data stays direct. The server holds the venue
credentials; this side holds only the server's access token, found in the local
registry for a loopback address (`synpath.server.local`) or given.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx

from .errors import AuthenticationError, BadRequest, ExchangeError, NetworkError, NotSupported
from .server.local import resolve_key
from .trading.errors import InsufficientFunds, OrderNotFound, RiskRejected
from .bucket import BUCKET_PREFIX, Bucket, BucketMember, BucketOrderReport, BucketPosition
from .trading.types import Balance, EditRequest, FeeEstimate, Fill, Order, OrderRequest, Position, Settlement, Side
from .types import Page

TRADING_PATH = "/trading"


class RemoteTrading:
    """The trading half of `Client`, over HTTP to a self-hosted server."""

    def __init__(self, server: str, key: str | None = None, *, http_client: Any = None,
                 home_dir: str | None = None, timeout: float = 30.0):
        base = server.rstrip("/")
        self.base = base if base.endswith(TRADING_PATH) else base + TRADING_PATH
        self.server = server
        self.key = resolve_key(server, key, home_dir=home_dir)
        self._http = http_client
        self._owned = http_client is None
        self.timeout = timeout

    # -- transport ------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        if not self.key:
            raise AuthenticationError(
                f"no access token for {self.server}: pass access_token=, set SYNPATH_ACCESS_TOKEN, or, for a server on this "
                "machine, start it with `synpath serve` so its token is in ~/.synpath/servers.json"
            )
        return {"Authorization": f"Bearer {self.key}"}

    async def _call(self, method: str, path: str, *, params: dict[str, Any] | None = None,
                    json: Any = None) -> Any:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=self.timeout)
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        try:
            response = await self._http.request(method, self.base + path, params=clean, json=json, headers=self._headers())
        except httpx.HTTPError as exc:
            raise NetworkError(f"{method} {path}: {exc}") from exc
        if response.status_code >= 400:
            raise self._error(response, method, path)
        return response.json() if response.content else None

    @staticmethod
    def _error(response: httpx.Response, method: str, path: str) -> Exception:
        """The server's `{"error": {"code", "message", "details"}}` as the
        exception an in-process caller would have caught."""
        try:
            error = (response.json() or {}).get("error") or {}
        except ValueError:
            error = {}
        code = str(error.get("code") or "")
        message = str(error.get("message") or response.text or response.reason_phrase)
        details = error.get("details") or {}
        status = response.status_code
        if status in (401, 403):
            return AuthenticationError(message)
        if status == 404:
            return OrderNotFound(message)
        if code == "risk_rejected" or status == 409:
            return RiskRejected(message, rule=str(details.get("rule") or code or "rejected"))
        if code == "insufficient_funds":
            return InsufficientFunds(message)
        if status in (400, 422):
            return BadRequest(message)
        if details.get("retryable"):
            return NetworkError(f"{method} {path} -> {status}: {message}")
        return ExchangeError(f"{method} {path} -> {status}: {message}")

    # -- orders ---------------------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        return Order.model_validate(await self._call("POST", "/orders", json=request.model_dump(mode="json")))

    async def create_bucket(self, *, book: str, name: str, members: list[BucketMember]) -> Bucket:
        body = {"book": book, "name": name, "members": [m.model_dump(mode="json") for m in members]}
        return Bucket.model_validate(await self._call("POST", "/buckets", json=body))

    async def fetch_buckets(self, *, book: str | None = None, status: str = "active") -> list[Bucket]:
        page = await self._call("GET", "/buckets", params={"book": book, "status": status})
        return [Bucket.model_validate(row) for row in page.get("data", [])]

    async def fetch_bucket(self, bucket_id: str) -> Bucket:
        return Bucket.model_validate(await self._call("GET", f"/buckets/{_bare(bucket_id)}"))

    async def archive_bucket(self, bucket_id: str) -> Bucket:
        return Bucket.model_validate(await self._call("DELETE", f"/buckets/{_bare(bucket_id)}"))

    async def fetch_bucket_position(self, bucket_id: str, *, book: str | None = None) -> BucketPosition:
        return BucketPosition.model_validate(
            await self._call("GET", f"/buckets/{_bare(bucket_id)}/position", params={"book": book}))

    async def fetch_bucket_orders(self, bucket_id: str) -> list[BucketOrderReport]:
        page = await self._call("GET", f"/buckets/{_bare(bucket_id)}/orders")
        return [BucketOrderReport.model_validate(row) for row in page.get("data", [])]

    async def fetch_bucket_order(self, bucket_id: str, order_id: str) -> BucketOrderReport:
        return BucketOrderReport.model_validate(
            await self._call("GET", f"/buckets/{_bare(bucket_id)}/orders/{order_id}"))

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        out: list[Order | Exception] = []
        for request in requests:
            try:
                out.append(await self.create_order(request))
            except Exception as exc:   # one refusal does not stop the rest, as the venue adapters behave
                out.append(exc)
        return out

    async def cancel_order(self, order_id: str, *, market_id: str | None = None, venue: str | None = None) -> Order:
        return Order.model_validate(await self._call("DELETE", f"/orders/{order_id}"))

    async def cancel_all_orders(self, *, market_id: str | None = None, venue: str | None = None) -> int | None:
        count = 0
        for order in await self.fetch_open_orders(market_id=market_id, venue=venue):
            await self.cancel_order(order.id)
            count += 1
        return count

    async def edit_order(self, request: EditRequest, *, venue: str | None = None, current: Order | None = None) -> Order:
        body = request.model_dump(mode="json", exclude_none=True)
        return Order.model_validate(await self._call("PATCH", f"/orders/{request.order_id}", json=body))

    async def fetch_order(self, order_id: str, *, market_id: str | None = None, venue: str | None = None) -> Order:
        return Order.model_validate(await self._call("GET", f"/orders/{order_id}"))

    async def fetch_open_orders(self, *, market_id: str | None = None, venue: str | None = None) -> list[Order]:
        page = await self._call("GET", "/orders", params={"venue": venue})
        orders = [Order.model_validate(row) for row in page.get("data", [])]
        return [o for o in orders if market_id is None or o.market_id == market_id]

    # -- portfolio ------------------------------------------------------------

    async def fetch_my_trades(self, *, market_id: str | None = None, venue: str | None = None,
                              since: int | None = None, limit: int | None = None, cursor: str | None = None) -> Page[Fill]:
        page = await self._call("GET", "/fills", params={"since": since, "venue": venue})
        fills = [Fill.model_validate(row) for row in page.get("data", [])]
        if market_id is not None:
            fills = [f for f in fills if f.market_id == market_id]
        if limit is not None:
            fills = fills[:limit]
        return Page(fills, next_cursor=None)

    async def fetch_positions(self, *, market_id: str | None = None, venue: str | None = None) -> list[Position]:
        page = await self._call("GET", "/positions")
        positions = [Position.model_validate(row) for row in page.get("data", [])]
        return [p for p in positions
                if (market_id is None or p.market_id == market_id) and (venue is None or p.venue == venue)]

    async def fetch_balance(self, venue: str) -> Balance:
        page = await self._call("GET", "/balances")
        for row in page.get("data", []):
            balance = Balance.model_validate(row)
            if balance.venue == venue:
                return balance
        raise NotSupported(f"{self.server} reports no balance for {venue}: is that venue configured there?")

    async def fetch_settlements(self, **_: Any) -> Page[Settlement]:
        raise NotSupported("settlements are read from the venue, not through a synpath server; "
                           "use synpath.Client(load_credentials()) for them")

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        raise NotSupported("fee estimates are read from the venue, not through a synpath server; "
                           "use synpath.Client(load_credentials()) or fetch_fee_schedule() for them")

    # -- housekeeping ---------------------------------------------------------

    async def close(self) -> None:
        if self._http is not None and self._owned:
            await self._http.aclose()
            self._http = None


def _bare(bucket_id: str) -> str:
    """A bucket's id, whether given as `<id>` or as its market id `bucket:<id>`."""
    return bucket_id[len(BUCKET_PREFIX):] if bucket_id.startswith(BUCKET_PREFIX) else bucket_id
