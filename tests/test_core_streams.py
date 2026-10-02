"""Streams on the Rust core against the same streams in Python.

A stream hands each raw frame to the Rust core first (`handle_raw`) and to
its Python handler for whatever the core declines. These tests replay real
recorded traffic through a stream both ways -- once as shipped, once with the
core switched off -- and require the same events, the same books and the same
pending checks after every frame. Dropped and corrupted frames make the
replays hit the gap and resync paths too.
"""
from __future__ import annotations

import dataclasses
import json
import random
from decimal import Decimal
from pathlib import Path

import pytest

from synpath import _native
from synpath.ws import LocalBook
from synpath.ws.polymarket import PolymarketMarketStream

pytestmark = pytest.mark.skipif(_native.core is None, reason="the Rust core is not built")

SAMPLES = Path(__file__).parent / "samples" / "ws"


def transcript(name: str) -> list[dict]:
    return [json.loads(line) for line in (SAMPLES / name).read_text().splitlines() if line.strip()]


def drain(stream) -> list:
    events = []
    while not stream._queue.empty():
        events.append(stream._queue.get_nowait())
    return events


def comparable(event) -> tuple:
    fields = {f.name: getattr(event, f.name) for f in dataclasses.fields(event) if f.name != "received_at"}
    return type(event).__name__, fields


def book_state(stream) -> dict:
    return {
        token: (book.ready, book.timestamp, dict(book.bids), dict(book.asks))
        for token, book in stream.books.items()
    }


class Recorder:
    """Resubscriptions a stream asks for, without a connection."""

    def __init__(self, stream):
        self.tokens: list[str] = []

        async def resubscribe(token):
            self.tokens.append(token)

        stream._resubscribe = resubscribe
        stream._later = _run_now


def _run_now(coroutine):
    try:
        coroutine.send(None)
    except StopIteration:
        pass


def polymarket_stream(rows, *, rust: bool):
    stream = PolymarketMarketStream()
    if not rust:
        stream.handle_raw = lambda raw: None
    recorder = Recorder(stream)
    for row in rows:
        if row["dir"] == "out" and "assets_ids" in row["msg"]:
            for token in row["msg"]["assets_ids"]:
                stream.tokens.add(token)
                stream.books.setdefault(token, LocalBook())
    return stream, recorder


def frames(rows):
    for row in rows:
        if row["dir"] == "in":
            msg = row["msg"]
            yield msg if isinstance(msg, str) else json.dumps(msg)


def corrupt(frame: str, rng: random.Random) -> str:
    """Change one size in a price change, so the local book drifts from the
    venue's top of book the way a missed message would make it."""
    try:
        message = json.loads(frame)
    except ValueError:
        return frame
    items = message if isinstance(message, list) else [message]
    for item in items:
        if isinstance(item, dict) and item.get("price_changes"):
            change = rng.choice(item["price_changes"])
            if rng.random() < 0.5:
                change["size"] = str(Decimal(change["size"]) + rng.randint(1, 50))
            else:
                change["best_bid"] = rng.choice(["0.01", "0.99", "", "0"])
            return json.dumps(message)
    return frame


def replay_both(raw_frames, rows):
    rust, rust_rec = polymarket_stream(rows, rust=True)
    python, python_rec = polymarket_stream(rows, rust=False)
    for frame in raw_frames:
        rust._dispatch(frame)
        python._dispatch(frame)
        assert [comparable(e) for e in drain(rust)] == [comparable(e) for e in drain(python)], frame[:200]
        assert book_state(rust) == book_state(python)
        assert rust._unverified == python._unverified
        assert rust.markets == python.markets
    assert rust_rec.tokens == python_rec.tokens
    return rust


class TestPolymarket:
    def test_the_core_handles_book_frames(self):
        rows = transcript("polymarket_market_transcript.jsonl")
        stream, _ = polymarket_stream(rows, rust=True)
        declined = []
        handle = stream.handle
        stream.handle = lambda message: declined.append(message) or handle(message)
        all_frames = list(frames(rows))
        for frame in all_frames:
            stream._dispatch(frame)
        # Only the trades and the heartbeats reach Python.
        assert 0 < len(declined) < len(all_frames) / 10

    def test_the_recorded_transcript_matches(self):
        rows = transcript("polymarket_market_transcript.jsonl")
        stream = replay_both(list(frames(rows)), rows)
        assert all(book.ready for book in stream.books.values() if book.timestamp is not None)

    @pytest.mark.parametrize("seed", range(20))
    def test_dropped_and_corrupted_frames_match(self, seed):
        rng = random.Random(seed)
        rows = transcript("polymarket_market_transcript.jsonl")
        raw = []
        for frame in frames(rows):
            roll = rng.random()
            if roll < 0.08:
                continue                       # a missed message
            raw.append(corrupt(frame, rng) if roll < 0.14 else frame)
        replay_both(raw, rows)

    def test_the_corrupted_replays_reach_the_gap_path(self):
        """Guards the test above: its replays must actually exercise gaps."""
        gaps = 0
        for seed in range(20):
            rng = random.Random(seed)
            rows = transcript("polymarket_market_transcript.jsonl")
            stream, recorder = polymarket_stream(rows, rust=True)
            for frame in frames(rows):
                roll = rng.random()
                if roll < 0.08:
                    continue
                stream._dispatch(corrupt(frame, rng) if roll < 0.14 else frame)
            gaps += len(recorder.tokens)
        assert gaps > 20

    def test_a_frame_python_would_reject_is_left_to_python(self):
        rows = transcript("polymarket_market_transcript.jsonl")
        rust, _ = polymarket_stream(rows, rust=True)
        python, _ = polymarket_stream(rows, rust=False)
        token = next(iter(rust.tokens))
        bad = json.dumps({"event_type": "book", "asset_id": token, "bids": [{"price": "abc", "size": "1"}], "asks": []})
        for stream in (rust, python):
            stream._dispatch(bad)
        assert [comparable(e) for e in drain(rust)] == [comparable(e) for e in drain(python)]
        assert [e.state for e in drain(rust)] == []
        assert book_state(rust) == book_state(python)


def kalshi_stream(rows, *, rust: bool):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    from synpath.trading.credentials import KalshiCredentials
    from synpath.ws.kalshi import Channel, KalshiStream

    pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
    )
    stream = KalshiStream(KalshiCredentials(key_id="key-1", private_key_pem=pem, env="demo"))
    if not rust:
        stream.handle_raw = lambda raw: None
    commands: list = []

    async def send_command(cmd, params):
        commands.append((cmd, params))

    stream._send_command = send_command
    stream._later = _run_now
    for row in rows:
        if row["dir"] == "out" and row["msg"].get("cmd") == "subscribe":
            params = row["msg"]["params"]
            channel = Channel(params["channels"][0], set(params.get("market_tickers") or []) or None)
            stream.channels[channel.name] = channel
            stream._by_command[row["msg"]["id"]] = channel
            if channel.name == "orderbook_delta":
                for ticker in channel.tickers or []:
                    stream.books.setdefault(ticker, LocalBook())
    return stream, commands


def kalshi_book_state(stream) -> dict:
    return {
        ticker: (book.ready, book.sequence, book.timestamp, dict(book.bids), dict(book.asks))
        for ticker, book in stream.books.items()
    }


def replay_kalshi_both(raw_frames, rows):
    rust, rust_commands = kalshi_stream(rows, rust=True)
    python, python_commands = kalshi_stream(rows, rust=False)
    for frame in raw_frames:
        rust._dispatch(frame)
        python._dispatch(frame)
        assert [comparable(e) for e in drain(rust)] == [comparable(e) for e in drain(python)], frame[:200]
        assert kalshi_book_state(rust) == kalshi_book_state(python)
        assert {n: c.last_seq for n, c in rust.channels.items()} == {n: c.last_seq for n, c in python.channels.items()}
    assert rust_commands == python_commands
    return rust, rust_commands


class TestKalshi:
    ROWS = "kalshi_demo_transcript.jsonl"

    def kalshi_frames(self, rows):
        return [json.dumps(row["msg"]) for row in rows if row["dir"] == "in"]

    def test_the_core_handles_book_frames(self):
        rows = transcript(self.ROWS)
        stream, _ = kalshi_stream(rows, rust=True)
        declined = []
        handle = stream.handle
        stream.handle = lambda message: declined.append(message) or handle(message)
        frames_in = self.kalshi_frames(rows)
        for frame in frames_in:
            stream._dispatch(frame)
        book_frames = [f for f in frames_in if '"orderbook_' in f]
        assert book_frames and not any(m.get("type", "").startswith("orderbook_") for m in declined)

    def test_the_recorded_transcript_matches(self):
        rows = transcript(self.ROWS)
        stream, _ = replay_kalshi_both(self.kalshi_frames(rows), rows)
        assert any(book.ready for book in stream.books.values())

    @pytest.mark.parametrize("seed", range(20))
    def test_dropped_frames_match(self, seed):
        """A dropped delta is a sequence gap: the book is invalidated, deltas
        are ignored until a snapshot, and a snapshot is asked for."""
        rng = random.Random(seed)
        rows = transcript(self.ROWS)
        raw = [frame for frame in self.kalshi_frames(rows) if rng.random() > 0.05]
        _, commands = replay_kalshi_both(raw, rows)

    def test_the_drops_reach_the_gap_path(self):
        rows = transcript(self.ROWS)
        asked = 0
        for seed in range(20):
            rng = random.Random(seed)
            stream, commands = kalshi_stream(rows, rust=True)
            for frame in self.kalshi_frames(rows):
                if rng.random() > 0.05:
                    stream._dispatch(frame)
            asked += sum(1 for cmd, params in commands if params.get("action") == "get_snapshot")
        assert asked > 10


def random_levels(rng, count, low, high):
    return [(Decimal(rng.randint(low, high)) / 100, Decimal(rng.randint(1, 500)) / 10) for _ in range(count)]


class TestOpinion:
    """One changed level per message, against books seeded from a snapshot;
    a token mid-snapshot holds its changes back for the replay."""

    YES, NO = "111", "222"

    def stream(self, rng, *, rust: bool):
        from synpath.ws.opinion import OpinionMarketStream, Watched

        stream = OpinionMarketStream(api_key="test-key-0123", catalog=None, resync_interval=None)
        if not rust:
            stream.handle_raw = lambda raw: None
        watched = Watched(market_id="opinion:8453", native="8453", yes_token=self.YES, no_token=self.NO, topic=None)
        seed = random.Random(rng.random())
        for token in (self.YES, self.NO):
            stream.tokens[token] = watched
            book = LocalBook()
            book.replace(random_levels(seed, 6, 1, 49), random_levels(seed, 6, 51, 99))
            stream.books[token] = book
        return stream

    def frames(self, rng, count=400):
        out = []
        for _ in range(count):
            roll = rng.random()
            token = rng.choice([self.YES, self.NO, "999"])     # 999 is not watched
            change = {"marketId": 8453, "tokenId": token, "side": rng.choice(["bids", "asks", "BIDS"]),
                      "price": str(Decimal(rng.randint(1, 99)) / 100), "size": str(rng.choice([0, 0, 5, 12.5, 40])),
                      "msgType": "market.depth.diff"}
            if roll < 0.03:
                change["price"] = "garbage"
            elif roll < 0.06:
                change = {"msgType": "market.last.price", "tokenId": token, "price": "0.5"}
            out.append(json.dumps(change))
        return out

    @pytest.mark.parametrize("seed", range(10))
    def test_random_changes_match(self, seed):
        rng = random.Random(seed)
        rust, python = self.stream(random.Random(seed), rust=True), self.stream(random.Random(seed), rust=False)
        for i, frame in enumerate(self.frames(rng)):
            if i == 150:                    # a snapshot is being read: changes queue up
                for stream in (rust, python):
                    stream._pending[self.YES] = []
            if i == 250:
                for stream in (rust, python):
                    stream.books[self.NO].invalidate()
            rust._dispatch(frame)
            python._dispatch(frame)
            assert [comparable(e) for e in drain(rust)] == [comparable(e) for e in drain(python)], frame
            assert book_state(rust) == book_state(python)
        assert rust._pending == python._pending and rust._pending[self.YES]


class TestPolymarketUS:
    SLUGS = ["tec-a", "tec-b"]

    def stream(self, *, rust: bool):
        import base64

        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import ed25519

        from synpath.trading.credentials import PolymarketUSCredentials
        from synpath.ws.polymarket_us import PolymarketUSMarketStream

        raw = ed25519.Ed25519PrivateKey.generate().private_bytes(
            serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption(),
        )
        stream = PolymarketUSMarketStream(PolymarketUSCredentials(key_id="kid", secret_key=base64.b64encode(raw).decode()))
        if not rust:
            stream.handle_raw = lambda raw: None
        return stream

    def frames(self, rng, count=200):
        out = []
        for _ in range(count):
            roll = rng.random()
            slug = rng.choice(self.SLUGS)
            if roll < 0.05:
                out.append(json.dumps({"heartbeat": {}}))
                continue
            if roll < 0.10:
                out.append(json.dumps({"marketDataLite": {"marketSlug": slug, "bestBid": {"value": "0.4"}}}))
                continue
            data = {
                "marketSlug": slug,
                "bids": [{"px": {"value": str(p), "currency": "USD"}, "qty": str(s)} for p, s in random_levels(rng, rng.randint(0, 5), 1, 49)],
                "offers": [{"px": str(p), "qty": str(s)} for p, s in random_levels(rng, rng.randint(0, 5), 51, 99)],
                "state": rng.choice(["MARKET_STATE_OPEN", "MARKET_STATE_HALTED", None]),
                "stats": {"lastTradePx": {"value": "0.55"}},
                "transactTime": rng.choice(["2026-09-17T12:00:00.123456789Z", "2026-09-17T12:00:00Z", None]),
            }
            key = "marketData" if rng.random() < 0.8 else "market_data"
            if roll < 0.13:
                data["bids"] = [{"px": "x", "qty": "1"}]
            out.append(json.dumps({"requestId": "md-1", key: data}))
        return out

    @pytest.mark.parametrize("seed", range(10))
    def test_random_market_data_matches(self, seed):
        rng = random.Random(seed)
        rust, python = self.stream(rust=True), self.stream(rust=False)
        for frame in self.frames(rng):
            rust._dispatch(frame)
            python._dispatch(frame)
            assert [comparable(e) for e in drain(rust)] == [comparable(e) for e in drain(python)], frame
            assert book_state(rust) == book_state(python)
            assert rust.states == python.states
