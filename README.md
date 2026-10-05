<div align="center">

<img src="https://raw.githubusercontent.com/Synpath-ai/synpath/main/assets/synpath-banner.png" alt="Synpath: one API for every prediction market" width="100%">

# Synpath – One API for prediction markets <a href="https://x.com/Synpath_Dev"><img src="https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2FSynpath_Dev&style=social&label=Follow" alt="Follow @Synpath_Dev on X" height="28"></a>

**Build on prediction markets without building against each one.**<br/>
One open-source API across Kalshi, Polymarket, Polymarket US and Opinion: market data, order entry, live streams, and an execution engine that remembers what it sent. In-process from Python, over REST and WebSocket from anywhere.

<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
<a href="https://pepy.tech/projects/synpath"><img src="https://img.shields.io/pepy/dt/synpath?label=Total%20Downloads&color=blue" alt="Total Downloads"></a>
<a href="https://synpath.dev/discord"><img src="https://img.shields.io/badge/Discord-join-5865F2?logo=discord&logoColor=white" alt="Discord"></a>

</div>

**🆕 New: Smart order routing across Kalshi and Polymarket.**<br/>
One order, both order books combined, filled from the cheapest price after fees. **[See how it works →](https://www.synpath.dev/docs/concepts/buckets)**

⭐ **If Synpath saves you time, [star it on GitHub](https://github.com/Synpath-ai/synpath).** It's how other traders find it.

---

## Supported Exchanges

| | Exchange | id | Market data | Order entry | Streams | Order books |
|:-:|---|---|:-:|:-:|:-:|---|
| <img src="assets/kalshi.png" width="20" height="20" alt="Kalshi"> | [Kalshi](https://kalshi.com) | `kalshi` | ✓ | ✓ | WebSocket | one book per market, both sides read it |
| <img src="assets/polymarket.png" width="20" height="20" alt="Polymarket"> | [Polymarket](https://polymarket.com) | `polymarket` | ✓ | ✓ | WebSocket | one book per outcome token |
| <img src="assets/polymarket.png" width="20" height="20" alt="Polymarket US"> | [Polymarket US](https://polymarket.us) | `polymarket_us` | ✓ | ✓ retail and exchange APIs | WebSocket and gRPC | one book per market, both sides read it |
| | [Opinion](https://opinion.trade) | `opinion` | ✓ | ✓ not yet live-tested | WebSocket (API key) | one book per outcome token |
| | [Hyperliquid](https://app.hyperliquid.xyz) | `hyperliquid` | ✓ outcome markets (HIP-4) | ✓ testnet-checked | WebSocket (no key) | one book per market, both sides read it |
| | [predict.fun](https://predict.fun) | `predict_fun` | ✓ (API key) | ✓ not yet live-tested | WebSocket (API key) | one book per market, both sides read it |

## Why Synpath

- **Liquidity is fragmented. Your time shouldn't be.** The same market trades on Kalshi, Polymarket, Polymarket US and Opinion, each with its own API, units and quirks. Synpath gives you one interface for all of them, and smart order routing buys from whichever book is cheapest. Spend your time on alpha, not plumbing.

- **Traders deserve advanced order types.** Stops, trailing stops, icebergs, OCO, brackets, TWAP and pegs, on every venue, even where the exchange has none. Orders are journaled before they're sent, so a crash never places one twice.

- **A home after Dome and pmxt.** Dome's API shut down in April 2026, and pmxt hasn't shipped since July 2026. Synpath is MIT-licensed, actively maintained, and follows ccxt conventions.

## Installation

```bash
pip install synpath              # market data, order entry, streams, the engine and the server
pip install "synpath[grpc]"      # + Polymarket US exchange gRPC streams
```

Python 3.10 or newer. The hot paths (order books, reading the venues' book streams, order signing and the order router) run on a Rust core that ships prebuilt inside the wheel for Linux, macOS and Windows; nothing else to install.

**Hosted API.** Run `synpath login`, then `synpath keys create`. It serves tick-level Kalshi order book and trade history, and cross-venue market matching. Trading stays on your machine: Synpath never holds your keys or funds.

## Quick Start

**Markets and quotes**

```python
import synpath

kalshi = synpath.Kalshi()
market = kalshi.fetch_markets(limit=1)[0]

print(market.title)                                # Will Elon Musk visit Mars before Aug 1, 2099?
print(market.yes.quote.bid, market.yes.quote.ask)  # 0.1 0.12
```

**Order books and search**

```python
book = kalshi.fetch_order_book(market.id, depth=5)
book.best_bid, book.best_ask                       # best first on both sides
kalshi.fetch_order_book(market.id, side="no")      # what NO costs

markets = kalshi.fetch_markets(query="trump", limit=10)
fee = kalshi.fetch_fee_schedule(markets[0].id)
fee.estimate(price=0.50, contracts=100)            # 1.75
```

**Same code, every venue**

```python
for venue_id in synpath.exchanges:                 # ['kalshi', 'polymarket', 'polymarket_us', 'opinion', 'hyperliquid', 'predict_fun']
    with synpath.exchange(venue_id) as venue:
        page = venue.fetch_markets(limit=5)

client = synpath.Client()                          # or one client, routed by the id
client.fetch_market("polymarket:2252244")          # every id starts with its venue
```

**Order entry**

```python
from decimal import Decimal
from synpath import KalshiTrading, OrderRequest, Side, load_credentials, require

async with KalshiTrading(require("kalshi", load_credentials())) as kalshi:
    order = await kalshi.create_order(OrderRequest(
        market_id=market.id, side=Side.BUY,          # buy takes YES, sell takes NO
        amount=Decimal("10"), price=Decimal("0.42"), # always the YES price
    ))
```

**Live streams**

```python
from synpath import PolymarketMarketStream, BookEvent

async with PolymarketMarketStream() as stream:
    await stream.watch_order_book(["polymarket:2252244"])   # both sides of the market
    async for event in stream:
        if isinstance(event, BookEvent):
            print(event.market_id, event.side, event.best_bid, event.best_ask)
```

**Smart order routing: one order across Kalshi and Polymarket**

```python
from synpath import Bucket, BucketMember, OrderType, PolymarketTrading
from synpath.engine import Engine, EngineConfig

creds = load_credentials()
async with KalshiTrading(require("kalshi", creds)) as kalshi, \
           PolymarketTrading(require("polymarket", creds)) as poly, \
           Engine({"kalshi": kalshi, "polymarket": poly}, EngineConfig(journal_path="trading.db")) as engine:
    # The same market on both venues; flip=True where a venue asks the question the other way round
    bucket = await engine.save_bucket(Bucket(book="alpha", name="Fed cut in December", members=[
        BucketMember(market_id="kalshi:KXFEDDECISION-26DEC-C25"),
        BucketMember(market_id="polymarket:2252244", flip=True),
    ]))
    # Both order books combined, each level net of fees, filled from the cheapest price outward
    await engine.submit(OrderRequest(
        market_id=bucket.market_id, side=Side.BUY, amount=Decimal("500"),
        type=OrderType.MARKET, price=Decimal("0.45"),   # the worst price you accept
        book="alpha",
    ))
```

**REST server**

```bash
python -m synpath.server                           # http://127.0.0.1:8000/docs
curl "localhost:8000/venues/kalshi/markets?limit=1"
```

Trading has its own app, with per-account keys and an append-only audit log:

```bash
python -m synpath.server bootstrap --control control.db     # the first key
python -m synpath.server schema --trading --out openapi.json
npx openapi-typescript openapi.json --default-non-nullable false -o src/synpath.d.ts
```

## Documentation

See the [API Reference](https://www.synpath.dev/docs) for detailed documentation and more examples.

## Development

```bash
git clone https://github.com/Synpath-ai/synpath
cd synpath
pip install -e ".[dev]"   # builds the Rust core too: needs a Rust toolchain (rustup.rs)

pytest              # offline, against recorded venue payloads
pytest -m live      # market data against the real venues
pytest -m demo      # order entry on a venue's demo environment
SYNPATH_PURE_PYTHON=1 pytest   # the same tests on the pure-Python fallback
cargo test -p synpath-core     # the Rust core on its own
```

The Rust core lives in `crates/synpath-core` (pure Rust) and
`bindings/python` (its PyO3 wrapper, built as `synpath._core`). Every class
it provides has a pure-Python twin with the same interface, and the library
uses the twin when the extension is not built. Venue adapters are plain
Python: adding a venue needs no Rust.

The `demo` tests place and cancel real orders on a demo exchange with the
credentials in your environment, and leave the account flat. `python -m
synpath.trading doctor` says which venues are configured without printing a
secret.

## Prior Art

[ccxt](https://github.com/ccxt/ccxt) set the conventions this library follows.

## License

MIT
