"""The Rust core against its pure-Python twin.

Every class in `synpath._core` stands in for a Python class with the same
interface. These tests run each behaviour on both, and replay random update
sequences through both side by side, so the two cannot drift apart. The rest
of the suite runs on whichever is installed; CI runs it once more with
`SYNPATH_PURE_PYTHON=1`.
"""
from __future__ import annotations

import os
import random
from decimal import Decimal as D

import pytest

from synpath import _native
from synpath.ws.base import BookLevel, PyLocalBook

RustLocalBook = getattr(_native.core, "LocalBook", None) if _native.core else None
if RustLocalBook is None and not _native.FORCED_PURE:
    try:
        from synpath._core import LocalBook as RustLocalBook  # type: ignore[no-redef]
    except ImportError:  # pragma: no cover
        RustLocalBook = None

IMPLEMENTATIONS = [pytest.param(PyLocalBook, id="python")]
if RustLocalBook is not None:
    IMPLEMENTATIONS.append(pytest.param(RustLocalBook, id="rust"))


@pytest.fixture(params=IMPLEMENTATIONS)
def Book(request):
    return request.param


def test_the_rust_core_is_in_use_unless_disabled():
    """A built package runs on the Rust core; only the switch turns it off."""
    from synpath.ws import LocalBook

    if os.environ.get("SYNPATH_PURE_PYTHON"):
        assert LocalBook is PyLocalBook and not _native.ENABLED
    else:
        assert _native.ENABLED, "synpath._core is not built: run `pip install -e .` with Rust installed"
        assert LocalBook is RustLocalBook


class TestLocalBook:
    def test_starts_empty_and_not_ready(self, Book):
        book = Book()
        assert not book.ready and book.best_bid is None and book.best_ask is None
        assert book.sequence is None and book.timestamp is None
        assert book.levels() == ((), ())

    def test_levels_best_and_removal(self, Book):
        book = Book()
        book.replace([(D("0.40"), D("10")), (D("0.42"), D("5")), (D("0.30"), D("0"))], [(D("0.45"), D("7"))])
        assert book.ready and book.best_bid == D("0.42") and book.best_ask == D("0.45")
        assert D("0.30") not in book.bids
        assert book.add("bid", D("0.42"), D("-5")) == 0 and book.best_bid == D("0.40")
        assert book.set("ask", D("0.44"), D("3")) == D("3") and book.best_ask == D("0.44")
        bids, asks = book.levels()
        assert bids == (BookLevel(D("0.40"), D("10")),)
        assert asks == (BookLevel(D("0.44"), D("3")), BookLevel(D("0.45"), D("7")))
        assert book.levels(1) == ((BookLevel(D("0.40"), D("10")),), (BookLevel(D("0.44"), D("3")),))

    def test_a_snapshot_accepts_any_iterable(self, Book):
        book = Book()
        book.replace(((D(p), D("1")) for p in ("0.1", "0.2")), iter([]))
        assert book.best_bid == D("0.2") and book.best_ask is None

    def test_mirrored_view(self, Book):
        book = Book()
        book.replace([(D("0.40"), D("10"))], [(D("0.45"), D("7"))])
        book.sequence, book.timestamp = 12, 1789000000000
        no = book.mirrored()
        assert no.best_bid == D("0.55") and no.best_ask == D("0.60") and no.bids[D("0.55")] == D("7")
        assert no.ready and no.sequence == 12 and no.timestamp == 1789000000000
        assert book.mirrored(D("100")).best_bid == D("99.55")

    def test_invalidate_keeps_the_levels(self, Book):
        book = Book()
        book.replace([(D("0.4"), D("1"))], [])
        book.invalidate()
        assert not book.ready and book.best_bid == D("0.4")
        book.ready = True
        assert book.ready

    def test_a_price_at_another_scale_is_the_same_level(self, Book):
        """The venue may send 0.4 in a snapshot and 0.40 in a delta."""
        book = Book()
        book.replace([(D("0.4"), D("10"))], [])
        assert book.add("bid", D("0.40"), D("-4")) == D("6")
        assert len(book.bids) == 1 and book.bids[D("0.4")] == D("6")

    def test_values_keep_their_exact_digits(self, Book):
        book = Book()
        book.replace([(D("0.4100"), D("12.50"))], [])
        (level,), _ = book.levels()
        assert str(level.price) == "0.4100" and str(level.size) == "12.50"


class TestSidesBehaveAsDicts:
    """`book.bids` and `book.asks` are dicts on the Python book; streams, tests
    and user code read and write them as such."""

    @pytest.fixture
    def book(self, Book):
        book = Book()
        book.replace([(D("0.40"), D("10")), (D("0.42"), D("5"))], [(D("0.45"), D("7"))])
        return book

    def test_read(self, book):
        assert len(book.bids) == 2
        assert book.bids[D("0.42")] == D("5")
        assert D("0.40") in book.bids and D("0.41") not in book.bids
        assert book.bids.get(D("0.41")) is None and book.bids.get(D("0.41"), D("0")) == D("0")
        assert sorted(book.bids) == [D("0.40"), D("0.42")]
        assert sorted(book.bids.items()) == [(D("0.40"), D("10")), (D("0.42"), D("5"))]
        assert sorted(book.bids.values()) == [D("5"), D("10")]
        assert dict(book.bids) == {D("0.40"): D("10"), D("0.42"): D("5")}

    def test_a_missing_price_is_a_key_error(self, book):
        with pytest.raises(KeyError):
            book.bids[D("0.99")]

    def test_compares_equal_to_a_plain_dict(self, book):
        assert book.bids == {D("0.4"): D("10"), D("0.42"): D("5")}
        assert book.asks != {D("0.45"): D("8")}
        assert book.asks != {}
        assert dict(book.bids) == book.bids

    def test_writes_reach_the_book(self, book):
        book.bids[D("0.43")] = D("2")
        assert book.best_bid == D("0.43")
        del book.bids[D("0.43")]
        assert book.best_bid == D("0.42")
        assert book.bids.pop(D("0.42")) == D("5") and book.best_bid == D("0.40")
        assert book.bids.pop(D("0.42"), None) is None
        with pytest.raises(KeyError):
            book.bids.pop(D("0.42"))

    def test_assigning_a_side_replaces_it(self, book):
        book.asks = {D("0.50"): D("1")}
        assert book.best_ask == D("0.50") and len(book.asks) == 1


def _apply(books, operation):
    for book in books:
        name, args = operation
        getattr(book, name)(*args)


def _state(book):
    bids, asks = book.levels()
    return (
        book.ready, book.best_bid, book.best_ask, dict(book.bids), dict(book.asks),
        bids, asks, book.levels(3), book.mirrored().levels(),
    )


@pytest.mark.skipif(RustLocalBook is None, reason="the Rust core is not built")
@pytest.mark.parametrize("seed", range(25))
def test_random_streams_leave_both_books_identical(seed):
    """Snapshots, sets, deltas that empty a level, invalidations, prices at
    different scales: whatever a stream does, the two books agree after
    every step."""
    rng = random.Random(seed)
    prices = [D(f"0.{n:02d}") for n in range(1, 100)] + [D("0.5"), D("0.50"), D("0.500")]

    def level():
        return rng.choice(prices), D(rng.randint(-3, 40)) / D(rng.choice([1, 10, 100]))

    python, rust = PyLocalBook(), RustLocalBook()
    for _ in range(300):
        roll = rng.random()
        if roll < 0.05:
            op = ("replace", ([level() for _ in range(rng.randint(0, 8))], [level() for _ in range(rng.randint(0, 8))]))
        elif roll < 0.55:
            op = ("set", (rng.choice(["bid", "ask"]), *level()))
        elif roll < 0.95:
            op = ("add", (rng.choice(["bid", "ask"]), *level()))
        else:
            op = ("invalidate", ())
        _apply((python, rust), op)
        assert _state(python) == _state(rust), op
