"""Old names kept working for one release, with a `DeprecationWarning`.

A number of contracts is `quantity` everywhere; it used to be `amount`. For
one release a caller who still writes the old name gets the new one and a
warning:

  * model input: `OrderRequest(amount=5)` is `OrderRequest(quantity=5)`
    (`renamed_fields`, which also gives the model a read-only `.amount`
    property; `model_dump()` and JSON only ever carry `quantity`);
  * keyword arguments: `fetch_fee_estimate(..., amount=5)`
    (`renamed_kwargs`);
  * plain attribute reads on dataclasses (`deprecated_property`).

TODO(0.9): remove this module and every use of it (search for
`_deprecated`).
"""
from __future__ import annotations

import functools
import inspect
import warnings
from typing import Any, Callable, Mapping, TypeVar

T = TypeVar("T")

QUANTITY = {"amount": "quantity"}
"""The rename every contract-count field and argument went through."""


def warn(old: str, new: str, *, stacklevel: int = 3) -> None:
    warnings.warn(f"`{old}` is deprecated, use `{new}`; it will be removed in the next release",
                  DeprecationWarning, stacklevel=stacklevel)


def rename_input(data: Any, renames: Mapping[str, str]) -> Any:
    """`data` (a model's raw input) with each old key under its new name."""
    if not renames or not isinstance(data, dict) or not any(old in data for old in renames):
        return data
    data = dict(data)
    for old, new in renames.items():
        if old in data:
            warn(old, new, stacklevel=5)
            value = data.pop(old)
            data.setdefault(new, value)
    return data


def deprecated_property(old: str, new: str) -> property:
    """A read-only `old` that returns `new`, with a warning."""
    def read(self: Any) -> Any:
        warn(old, new)
        return getattr(self, new)
    read.__name__ = old
    read.__doc__ = f"Deprecated: use `{new}`."
    return property(read)


def renamed_fields(renames: Mapping[str, str]) -> Callable[[type[T]], type[T]]:
    """Class decorator for a model whose fields were renamed. The model's
    base runs `rename_input` with `__synpath_renamed__` before validation;
    this adds the read-only old names."""
    def decorate(cls: type[T]) -> type[T]:
        merged = {**getattr(cls, "__synpath_renamed__", {}), **renames}
        setattr(cls, "__synpath_renamed__", merged)
        for old, new in renames.items():
            setattr(cls, old, deprecated_property(old, new))
        return cls
    return decorate


def renamed_kwargs(**renames: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Function decorator: an old keyword argument is taken as the new one."""
    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        def fix(kwargs: dict[str, Any]) -> None:
            for old, new in renames.items():
                if old in kwargs:
                    warn(old, new, stacklevel=4)
                    value = kwargs.pop(old)
                    if new in kwargs:
                        raise TypeError(f"{fn.__name__}() got both `{old}` and `{new}`")
                    kwargs[new] = value

        if inspect.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                fix(kwargs)
                return await fn(*args, **kwargs)
            return async_wrapper

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            fix(kwargs)
            return fn(*args, **kwargs)
        return wrapper
    return decorate
