"""One error shape for every route: `{"error": {"code", "message", "details"}}`.

Both apps install the same handlers, so a client writes one error path. The
`code` is derived from the library's exception (`InsufficientFunds` becomes
`insufficient_funds`), so an HTTP caller branches on the same vocabulary an
in-process caller catches; errors the HTTP layer raises itself (no token, no
permission, a malformed body) get codes from their status.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..errors import NetworkError, SynpathError
from .models import ErrorBody, ErrorDetail

log = logging.getLogger("synpath.server")

STATUS_CODES: dict[int, str] = {
    400: "bad_request", 401: "unauthorized", 403: "forbidden", 404: "not_found", 405: "method_not_allowed",
    409: "conflict", 422: "validation_error", 429: "rate_limited", 500: "internal_error", 501: "not_supported",
    502: "venue_error", 503: "unavailable", 504: "venue_timeout",
}


def code_of(exc: BaseException) -> str:
    """`InsufficientFunds` -> `insufficient_funds`."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", type(exc).__name__).lower()


def error_body(code: str, message: str, **details: Any) -> dict[str, Any]:
    details.setdefault("venue", None)
    details.setdefault("retryable", False)
    return ErrorBody(error=ErrorDetail(code=code, message=message, details=details)).model_dump()


def http_error(status: int, code: str, message: str, **details: Any) -> HTTPException:
    """An HTTPException that carries its own code, for routes that know better
    than the status alone (`unknown_venue` rather than `not_found`)."""
    return HTTPException(status_code=status, detail={"code": code, "message": message, "details": details})


def install_error_handlers(app: FastAPI, status_for: Callable[[Exception], int]) -> None:
    @app.exception_handler(SynpathError)
    async def _on_synpath_error(request: Request, exc: SynpathError) -> JSONResponse:
        status = status_for(exc)
        if status >= 500 and status != 501:
            log.warning("%s %s -> %s: %s", request.method, request.url.path, status, exc)
        from .api import _venue_of

        details: dict[str, Any] = {
            "venue": getattr(exc, "synpath_venue", None) or _venue_of(exc),
            "retryable": isinstance(exc, NetworkError),
        }
        for name in ("rule", "reason"):
            value = getattr(exc, name, None)
            if value:
                details[name] = value
        headers = {}
        retry_after = getattr(exc, "retry_after", None)
        if retry_after:
            details["retry_after"] = retry_after
            headers["Retry-After"] = str(int(retry_after))
        return JSONResponse(status_code=status, content=error_body(code_of(exc), str(exc), **details),
                            headers=headers)

    @app.exception_handler(StarletteHTTPException)
    async def _on_http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        detail = exc.detail
        if isinstance(detail, dict) and "code" in detail:
            body = error_body(detail["code"], str(detail.get("message", "")), **(detail.get("details") or {}))
        else:
            body = error_body(STATUS_CODES.get(exc.status_code, "error"), str(detail))
        return JSONResponse(status_code=exc.status_code, content=body, headers=getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def _on_invalid(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [{"field": ".".join(str(part) for part in e.get("loc", ()) if part != "body"),
                   "message": e.get("msg", "")} for e in exc.errors()]
        summary = "; ".join(f"{e['field']}: {e['message']}" for e in errors) or "the request is not valid"
        return JSONResponse(status_code=422, content=error_body("validation_error", summary, errors=errors))
