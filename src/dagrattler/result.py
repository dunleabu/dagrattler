"""Result container types used to move successes and errors through the graph."""

from __future__ import annotations

from dataclasses import dataclass


def ensure_exception(exc: BaseException) -> Exception:
    """Return ``exc`` as an ``Exception`` for storage in ``Err`` values.

    Args:
        exc: The raised ``BaseException`` to normalize.

    Returns:
        The original exception if it already subclasses ``Exception``, otherwise a
        generic ``Exception`` with the same message text.
    """

    if isinstance(exc, Exception):
        return exc
    return Exception(str(exc))


@dataclass(slots=True)
class Ok[T]:
    """Wrap a successful payload passed between nodes.

    Args:
        value: The successful value carried through the graph.

    Attributes:
        value: The wrapped payload.
    """

    value: T


@dataclass(slots=True)
class Err:
    """Wrap a failure passed between nodes.

    Args:
        error: The exception describing the failure.

    Attributes:
        error: The normalized ``Exception`` stored on the result.
    """

    error: Exception

    def __post_init__(self) -> None:
        """Normalize the stored error to an ``Exception`` instance."""

        self.error = ensure_exception(self.error)
