"""Shared account authorization for order responses and their audit events."""
from __future__ import annotations

from ..engine.execution_scope import MANAGED_NAMESPACE, order_accounts
from ..trading.types import Account, Order


def may_view_account(principal, account: str) -> bool:
    return principal.may("view", account) or principal.may("trade", account)


def may_view_order(principal, order: Order) -> bool:
    return all(may_view_account(principal, account) for account in order_accounts(order))


def may_trade_order(principal, order: Order) -> bool:
    return all(principal.may("trade", account) for account in order_accounts(order))


async def may_view_event(principal, engine, event) -> bool:
    """Resolve legacy parent events through durable identity, never nominal account.

    New snapshots carry scope inline. Old append-only events are unchanged;
    when their parent cannot be identified safely, only a global viewer sees
    them. Individual child events remain visible on their concrete account.
    """
    body = event.payload
    request = body.get("effective_request") or body.get("request") or {}
    if event.kind.startswith("submission."):
        reservation = await engine.journal.submission(event.key) if event.key else None
        if reservation and reservation["venue"] == MANAGED_NAMESPACE:
            captured = reservation["effective_request"]["params"]["execution_accounts"]
            return all(may_view_account(principal, Account.model_validate(account).key) for account in captured.values())
    is_parent = event.kind.startswith("managed.") or body.get("held_by") == "engine"
    # Request params are caller-controlled, even when recorded in an audit
    # event. Only engine-produced order snapshots or stored reservations own scope.
    scope = body.get("execution_accounts")
    if scope and is_parent:
        return all(may_view_account(principal, Account.model_validate(account).key) for account in scope.values())
    if is_parent:
        parent_id = body.get("parent_id") or body.get("id") or event.key
        if parent_id and parent_id.startswith(f"{MANAGED_NAMESPACE}:"):
            parent_id = parent_id.split(":", 1)[1]
        stored = await engine.journal.managed_one(parent_id) if parent_id else None
        if stored:
            parent = engine._rebuilt(*stored)
            return parent is not None and may_view_order(principal, parent.as_order())
        return principal.may("view", "*")
    account = body.get("account") or request.get("account")
    if isinstance(account, dict):
        return may_view_account(principal, Account.model_validate(account).key)
    if isinstance(account, str) and account != MANAGED_NAMESPACE:
        return may_view_account(principal, account)
    if scope:
        return all(may_view_account(principal, Account.model_validate(account).key) for account in scope.values())
    parent_id = body.get("parent_id") or (request.get("tags") or {}).get("parent")
    if parent_id:
        stored = await engine.journal.managed_one(parent_id)
        if stored:
            parent = engine._rebuilt(*stored)
            return parent is not None and may_view_order(principal, parent.as_order())
        return principal.may("view", "*")
    if event.kind.startswith(("order.", "fill.", "intent.", "submission.", "risk.")):
        # Legacy order events sometimes name only their client/native ID.
        key = body.get("client_order_id") or event.key
        order = await engine.journal.order_by_client_id(key) if key else None
        if order is None and key:
            try:
                order = await engine._known(key.split(":", 1)[-1], None)
            except Exception:
                pass
        return may_view_order(principal, order) if order else principal.may("view", "*")
    return True
