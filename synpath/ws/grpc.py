"""gRPC server streams as `Stream`s, and the venue protos they are read with.

The Polymarket US exchange API streams over gRPC rather than WebSockets. A
`GrpcStream` keeps the promises of `synpath.ws.base.Stream` -- typed events
on one queue, reconnects with jittered backoff, `gap`, `reconcile_required`,
a bad message is an `error` and not the end -- over a server-streaming RPC.

**Protos are the user's.** Polymarket publishes its `.proto` files as a
download without a license, so synpath ships neither the protos nor code
generated from them. Point a stream at the unzipped bundle (or the zip
itself), or set `SYNPATH_POLYMARKET_US_PROTOS`; the bundle is compiled once
into a descriptor set cached under `~/.cache/synpath/protos`, keyed by the
protos' contents, and messages are built from that at run time. Nothing is
written into the package or onto `sys.path`.

Messages are handed to venue code as dicts in the protos' JSON mapping
(camelCase names, int64s as strings, enums by name, timestamps as RFC 3339),
the same shapes the exchange's REST API returns, so one set of normalizers
reads both. Fields left at their proto3 default are written out, because
the default of an enum is a real value here (`ORDER_STATE_NEW`,
`INSTRUMENT_STATE_CLOSED`).
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import zipfile
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Literal

from .base import _CLOSED, Stream, log, now_ms

PROTO_ENV = "SYNPATH_POLYMARKET_US_PROTOS"

CallOpener = Callable[[str, dict[str, Any], list[tuple[str, str]]], Awaitable[AsyncIterator[Any]]]
"""`(method, request, metadata) -> responses`: how a stream opens one call.
The default speaks gRPC; tests pass scripted responses."""

READY = object()
"""Yielded by a call once the server has answered (its headers arrived)."""

Outcome = Literal["retry", "reauthenticate", "slow_down", "fatal"]

RETRY_CODES = {"UNAVAILABLE", "INTERNAL", "UNKNOWN", "DEADLINE_EXCEEDED", "ABORTED", "CANCELLED", "DATA_LOSS"}
FATAL_CODES = {"PERMISSION_DENIED", "INVALID_ARGUMENT", "UNIMPLEMENTED", "NOT_FOUND", "FAILED_PRECONDITION", "OUT_OF_RANGE"}


class ProtosMissing(RuntimeError):
    """The venue's protos were not given or cannot be compiled."""


# ---------------------------------------------------------------------------
# Protos
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RpcMethod:
    path: str
    request: type
    response: type


def _cache_root() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
    return Path(base) / "synpath" / "protos"


def _find_root(path: Path, anchor: str) -> Path | None:
    if (path / anchor).is_file():
        return path
    for current, dirs, _files in os.walk(path):
        dirs[:] = sorted(d for d in dirs if not d.startswith("."))
        if (Path(current) / anchor).is_file():
            return Path(current)
    return None


class ProtoBundle:
    """A venue's protos, compiled into a private descriptor pool.

    `path` is a directory holding the bundle (at any depth) or the zip it was
    downloaded as. `anchor` is a file whose presence marks the import root.
    """

    _loaded: dict[tuple[str, str], "ProtoBundle"] = {}

    def __init__(self, path: str | os.PathLike[str] | None = None, *, anchor: str, files: tuple[str, ...], cache_dir: str | os.PathLike[str] | None = None):
        try:
            from google.protobuf import descriptor_pb2, descriptor_pool  # noqa: F401
        except ImportError as exc:  # pragma: no cover - depends on the environment
            raise ImportError("gRPC streams need grpcio and protobuf: pip install synpath[grpc]") from exc
        given = path if path is not None else os.environ.get(PROTO_ENV)
        if not given:
            raise ProtosMissing(
                f"the Polymarket US exchange protos are needed for its gRPC streams: download the bundle from the "
                f"venue's documentation and pass its directory or zip as `protos=`, or set {PROTO_ENV}"
            )
        self.cache_dir = Path(cache_dir) if cache_dir is not None else _cache_root()
        source = Path(given).expanduser()
        if source.is_file() and zipfile.is_zipfile(source):
            source = self._unzip(source)
        if not source.is_dir():
            raise ProtosMissing(f"no proto bundle at {given}")
        root = _find_root(source, anchor)
        if root is None:
            raise ProtosMissing(f"{given} does not contain {anchor}")
        self.root = root
        self.files = files
        self.pool = self._pool(self._descriptor_set())
        self._classes: dict[str, type] = {}
        self._methods: dict[str, RpcMethod] = {}

    @classmethod
    def load(cls, path: str | os.PathLike[str] | None = None, **kwargs: Any) -> "ProtoBundle":
        """One bundle per location per process."""
        key = (str(path or os.environ.get(PROTO_ENV) or ""), repr(sorted(kwargs.items())))
        bundle = cls._loaded.get(key)
        if bundle is None:
            bundle = cls._loaded[key] = cls(path, **kwargs)
        return bundle

    def _unzip(self, archive: Path) -> Path:
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()[:16]
        target = self.cache_dir / f"bundle-{digest}"
        if not target.is_dir():
            staging = self.cache_dir / f".bundle-{digest}-{os.getpid()}"
            with zipfile.ZipFile(archive) as zf:
                base = staging.resolve()
                for member in zf.namelist():
                    if not (base / member).resolve().is_relative_to(base):
                        raise ProtosMissing(f"{archive} has an entry outside the archive: {member}")
                zf.extractall(staging)
            if target.exists():
                # Another process unpacked the same archive first.
                import shutil

                shutil.rmtree(staging, ignore_errors=True)
            else:
                staging.replace(target)
        return target

    def _descriptor_set(self) -> bytes:
        protos = sorted(p for p in self.root.rglob("*.proto") if p.is_file())
        digest = hashlib.sha256()
        for proto in protos:
            digest.update(str(proto.relative_to(self.root)).encode())
            digest.update(proto.read_bytes())
        digest.update(repr(self.files).encode())
        out = self.cache_dir / f"{digest.hexdigest()[:24]}.pb"
        if out.is_file():
            return out.read_bytes()
        try:
            from importlib.resources import files as resource_files

            from grpc_tools import protoc
        except ImportError as exc:  # pragma: no cover - depends on the environment
            raise ImportError("compiling the venue's protos needs grpcio-tools: pip install synpath[grpc]") from exc
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        staging = out.with_suffix(f".{os.getpid()}.tmp")
        include = str(resource_files("grpc_tools") / "_proto")
        missing = [f for f in self.files if not (self.root / f).is_file()]
        if missing:
            raise ProtosMissing(f"the bundle at {self.root} has no {missing[0]}")
        code = protoc.main([
            "protoc", f"-I{self.root}", f"-I{include}", "--include_imports", f"--descriptor_set_out={staging}", *self.files,
        ])
        if code != 0 or not staging.is_file():
            raise ProtosMissing(f"protoc could not compile the bundle at {self.root} (exit {code})")
        staging.replace(out)
        return out.read_bytes()

    @staticmethod
    def _pool(data: bytes) -> Any:
        from google.protobuf import descriptor_pb2, descriptor_pool

        pool = descriptor_pool.DescriptorPool()
        for proto in descriptor_pb2.FileDescriptorSet.FromString(data).file:
            pool.Add(proto)
        return pool

    def message(self, full_name: str) -> type:
        cls = self._classes.get(full_name)
        if cls is None:
            from google.protobuf import message_factory

            cls = self._classes[full_name] = message_factory.GetMessageClass(self.pool.FindMessageTypeByName(full_name))
        return cls

    def method(self, name: str) -> RpcMethod:
        """`"package.Service/Method"` as a path and its message classes."""
        found = self._methods.get(name)
        if found is None:
            service, _, rpc = name.partition("/")
            descriptor = self.pool.FindServiceByName(service).FindMethodByName(rpc)
            found = self._methods[name] = RpcMethod(
                path=f"/{service}/{rpc}",
                request=self.message(descriptor.input_type.full_name),
                response=self.message(descriptor.output_type.full_name),
            )
        return found

    @staticmethod
    def to_dict(message: Any) -> dict[str, Any]:
        from google.protobuf import json_format

        return json_format.MessageToDict(message, always_print_fields_with_no_presence=True)

    @staticmethod
    def from_dict(cls: type, data: dict[str, Any]) -> Any:
        from google.protobuf import json_format

        return json_format.ParseDict(data, cls())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class RecentIds:
    """The last `capacity` keys seen, for at-least-once streams."""

    def __init__(self, capacity: int = 50_000):
        self.capacity = capacity
        self._keys: OrderedDict[Any, None] = OrderedDict()

    def seen(self, key: Any) -> bool:
        """True if `key` was already seen; records it either way."""
        if key in self._keys:
            self._keys.move_to_end(key)
            return True
        self._keys[key] = None
        if len(self._keys) > self.capacity:
            self._keys.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._keys)


def status_code(exc: BaseException) -> str | None:
    """The gRPC status name of an error, if it is an RPC error."""
    code = getattr(exc, "code", None)
    if not callable(code):
        return None
    try:
        value = code()
    except Exception:
        return None
    return getattr(value, "name", None) or (str(value) if value is not None else None)


def status_details(exc: BaseException) -> str:
    details = getattr(exc, "details", None)
    if callable(details):
        try:
            return str(details() or "")
        except Exception:
            return ""
    return str(exc)


class IdleTimeout(Exception):
    pass


class StreamEnded(Exception):
    pass


# ---------------------------------------------------------------------------
# Stream
# ---------------------------------------------------------------------------

class GrpcStream(Stream):
    """One server-streaming RPC, kept alive, turned into events.

    Subclasses name the RPC (`method`), build the request (`request()`,
    rebuilt for every call so a resume point or a new symbol set goes out),
    give the call's metadata (`metadata()`), may do asynchronous work before
    reading a message (`prepare()`), and read it (`handle()`, on the message
    as a dict). `resubscribe()` ends the current call and opens a new one.
    """

    method: str = ""

    def __init__(
        self,
        target: str,
        *,
        protos: ProtoBundle | None = None,
        call: CallOpener | None = None,
        keepalive_s: float = 300.0,
        insecure: bool = False,
        **kwargs: Any,
    ):
        super().__init__(target, **kwargs)
        self.insecure = insecure
        """Plaintext, for a local test server only."""
        self.protos = protos
        self._call_opener = call
        self.keepalive_s = keepalive_s
        self._channel: Any = None
        self._grpc_call: Any = None
        self._wake = asyncio.Event()
        self._resubscribe = asyncio.Event()
        self.last_status: str | None = None

    # -- venue hooks ----------------------------------------------------------

    def request(self) -> dict[str, Any] | None:
        """The request for the next call; `None` waits for `resubscribe()`."""
        return {}

    async def metadata(self) -> list[tuple[str, str]]:
        return []

    async def before_call(self, request: dict[str, Any]) -> dict[str, Any] | None:
        """Asynchronous work before a call opens; may return a changed request,
        or `None` to open none and wait for `resubscribe()`."""
        return request

    async def prepare(self, message: dict[str, Any]) -> None:
        """Asynchronous work before `handle` reads a message (loading scales)."""

    def reconcile_on_reconnect(self) -> bool:
        return self.private

    def classify(self, code: str | None) -> Outcome:
        if code == "UNAUTHENTICATED":
            return "reauthenticate"
        if code == "RESOURCE_EXHAUSTED":
            return "slow_down"
        if code in FATAL_CODES:
            return "fatal"
        return "retry"

    async def on_reauthenticate(self) -> None:
        """Forget the access token so the next call gets a fresh one."""

    # -- lifecycle ------------------------------------------------------------

    def resubscribe(self) -> None:
        """Open the call again with a fresh `request()`."""
        self._resubscribe.set()
        self._wake.set()

    async def send(self, frame: Any) -> bool:  # pragma: no cover - server streams take no frames
        raise NotImplementedError("a server-streaming RPC takes no frames after its request")

    async def close(self) -> None:
        await super().close()
        channel, self._channel = self._channel, None
        if channel is not None:
            try:
                await channel.close()
            except Exception:  # pragma: no cover - closing a broken channel
                pass

    async def _open_call(self, request: dict[str, Any], metadata: list[tuple[str, str]]) -> AsyncIterator[Any]:
        if self._call_opener is not None:
            return await self._call_opener(self.method, request, metadata)
        if self.protos is None:
            raise ProtosMissing("no protos loaded")
        import grpc

        if self._channel is None:
            options = [
                ("grpc.keepalive_time_ms", int(self.keepalive_s * 1000)),
                ("grpc.keepalive_timeout_ms", 20_000),
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ]
            if self.insecure:
                self._channel = grpc.aio.insecure_channel(self.url, options=options)
            else:
                self._channel = grpc.aio.secure_channel(self.url, grpc.ssl_channel_credentials(), options=options)
        rpc = self.protos.method(self.method)
        multi = self._channel.unary_stream(
            rpc.path, request_serializer=rpc.request.SerializeToString, response_deserializer=rpc.response.FromString,
        )
        grpc_call = multi(self.protos.from_dict(rpc.request, request), metadata=metadata)
        self._grpc_call = grpc_call
        return self._responses(grpc_call)

    async def _responses(self, grpc_call: Any) -> AsyncIterator[Any]:
        try:
            await grpc_call.initial_metadata()
            # A call refused outright (bad token, too many streams) is already
            # finished when its headers resolve: that is not a connection.
            if not grpc_call.done():
                yield READY
            async for response in grpc_call:
                yield response
        finally:
            grpc_call.cancel()

    def _to_dict(self, response: Any) -> dict[str, Any]:
        if isinstance(response, dict):
            return response
        if self.protos is not None:
            return self.protos.to_dict(response)
        from google.protobuf import json_format  # pragma: no cover - a message without a bundle

        return json_format.MessageToDict(response, always_print_fields_with_no_presence=True)

    async def _consume(self, responses: AsyncIterator[Any], on_ready: Callable[[], None]) -> None:
        iterator = responses.__aiter__()
        while True:
            try:
                if self.idle_timeout:
                    item = await asyncio.wait_for(iterator.__anext__(), timeout=self.idle_timeout)
                else:
                    item = await iterator.__anext__()
            except StopAsyncIteration:
                raise StreamEnded("the venue ended the stream") from None
            except asyncio.TimeoutError:
                raise IdleTimeout(f"no message for {self.idle_timeout}s") from None
            on_ready()
            self._last_seen = now_ms()
            if item is READY:
                continue
            self.stats.messages += 1
            self.stats.last_message_at = now_ms()
            try:
                message = self._to_dict(item)
                await self.prepare(message)
                events = self.handle(message)
            except Exception as exc:
                log.debug("synpath.ws %s %s: unreadable message %r", self.venue, self.name, item, exc_info=True)
                self.status("error", f"unreadable message: {type(exc).__name__}: {exc}")
                continue
            for event in events:
                self.emit(event)

    def _fail(self, detail: str) -> None:
        self._closing = True
        self.status("failed", detail)
        self._queue.put_nowait(_CLOSED)

    async def _run(self) -> None:
        attempt = 0
        while not self._closing:
            self._resubscribe.clear()
            request = self.request()
            if request is not None:
                try:
                    request = await self.before_call(request)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.status("connect_failed", f"{type(exc).__name__}: {exc}")
                    await self._sleep(self.backoff(attempt))
                    attempt += 1
                    continue
            if request is None:
                self._wake.clear()
                if not self._resubscribe.is_set():
                    await self._wake.wait()
                continue
            try:
                metadata = await self.metadata()
                responses = await self._open_call(request, metadata)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                code = status_code(exc)
                self.last_status = code
                self.status("connect_failed", f"{code or type(exc).__name__}: {status_details(exc) if code else exc}")
                if await self._after_failure(code, f"{code}: {status_details(exc)}", attempt):
                    return
                attempt += 1
                continue

            confirmed = False

            def ready() -> None:
                nonlocal confirmed, attempt
                if confirmed:
                    return
                confirmed = True
                attempt = 0
                reconnect = self.stats.connects > 0
                self.stats.connects += 1
                self.connected.set()
                self.status("connected", "reconnected" if reconnect else "", reconcile=reconnect and self.reconcile_on_reconnect())

            self._last_seen, self._stale = now_ms(), ""
            loop = asyncio.get_running_loop()
            reader = loop.create_task(self._consume(responses, ready))
            waker = loop.create_task(self._resubscribe.wait())
            watching = loop.create_task(self._stale_after_sleep()) if self.idle_timeout else None
            watched = {reader, waker} | ({watching} if watching else set())
            code: str | None = None
            reason = "closed"
            resubscribing = False
            try:
                done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
                if reader in done:
                    exc = reader.exception()
                    code = status_code(exc) if exc else None
                    reason = f"{code}: {status_details(exc)}" if code else f"{type(exc).__name__}: {exc}" if exc else "ended"
                elif watching is not None and watching in done:
                    reason = self._stale
                else:
                    resubscribing = True
                    reason = "resubscribing"
            finally:
                for task in (reader, waker, watching):
                    if task is not None and not task.done():
                        task.cancel()
                        try:
                            await task
                        except (asyncio.CancelledError, Exception):
                            pass
                aclose = getattr(responses, "aclose", None)
                if aclose is not None:
                    try:
                        await aclose()
                    except (asyncio.CancelledError, Exception):
                        pass
                self._grpc_call = None
                if confirmed:
                    self.connected.clear()
                    self.stats.disconnects += 1
                self.on_disconnect()
            if self._closing:
                return
            self.last_status = code
            # A call the venue refused before answering was never a connection,
            # and one replaced before it answered was neither.
            if confirmed:
                self.status("disconnected", reason)
            elif not resubscribing:
                self.status("connect_failed", reason)
            if resubscribing:
                continue
            if await self._after_failure(code, reason, attempt):
                return
            if not confirmed:
                attempt += 1

    async def _after_failure(self, code: str | None, reason: str, attempt: int) -> bool:
        """Wait as the failure deserves; `True` when the stream must stop."""
        outcome = self.classify(code)
        if outcome == "fatal":
            self._fail(reason)
            return True
        if outcome == "reauthenticate":
            await self.on_reauthenticate()
            await self._sleep(self.backoff(attempt) if attempt else 0)
        elif outcome == "slow_down":
            await self._sleep(self.backoff_max)
        else:
            await self._sleep(self.backoff(attempt))
        return False

    async def _stale_after_sleep(self) -> None:
        """Wall-clock silence, for the same reason as the WebSocket streams'
        watchdog: a suspended machine freezes the event loop's timers while
        the connection dies underneath them."""
        step = max(1.0, (self.idle_timeout or 0) / 4)
        while True:
            await asyncio.sleep(step)
            silent = (now_ms() - self._last_seen) / 1000
            if self.idle_timeout and silent > self.idle_timeout:
                self._stale = f"no message for {round(silent)}s (wall clock)"
                return

    async def _sleep(self, seconds: float) -> None:
        """Back off, cut short by `resubscribe()`."""
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._resubscribe.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
