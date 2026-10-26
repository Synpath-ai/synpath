"""Execution accounts and private storage identity for routed instructions.

The managed namespace is a database key, never a venue or an Account.
Only children have a concrete execution venue; a routed parent saves the
complete set of accounts it is allowed to use.
"""
from __future__ import annotations

from ..trading.types import Account, Order
from ..errors import BadRequest

MANAGED_NAMESPACE = "__managed__"


class BucketNotFound(BadRequest):
    """A missing bucket remains a bad SDK request and an HTTP 404."""


def storage_identity(order: Order) -> tuple[str, str]:
    if order.execution_accounts:
        return MANAGED_NAMESPACE, MANAGED_NAMESPACE
    return order.venue, order.account.key if order.account else f"{order.venue}:default"


def order_accounts(order: Order) -> list[str]:
    if order.execution_accounts:
        return sorted(account.key for account in order.execution_accounts.values())
    return [order.account.key if order.account else f"{order.venue}:default"]


def validate_execution_accounts(accounts: dict[str, Account]) -> dict[str, Account]:
    if not accounts or any(venue != account.venue for venue, account in accounts.items()):
        raise ValueError("execution accounts must match their concrete venues")
    return dict(sorted(accounts.items()))
