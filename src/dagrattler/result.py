from __future__ import annotations

from dataclasses import dataclass


def ensure_exception(exc: BaseException) -> Exception:
    if isinstance(exc, Exception):
        return exc
    return Exception(str(exc))


@dataclass(slots=True)
class Ok[T]:
    value: T


@dataclass(slots=True)
class Err:
    error: Exception

    def __post_init__(self) -> None:
        self.error = ensure_exception(self.error)
