"""Reusable factory registrations; instance caches live in containers."""

from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock
from typing import Any, TypeVar, overload

from .errors import FrozenError

F = TypeVar("F", bound=Callable[..., Any])


@dataclass(frozen=True)
class _Provider:
    factory: Callable[..., Any]
    resource: bool


def _validate_name(name: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError("Object names must be nonempty strings")


class Registry:
    """Named factories, frozen by the first lookup in any attached container.

    Factories accept their owning container. ``resource=True`` means the
    factory returns a context manager whose lifetime belongs to that container.
    """

    def __init__(self) -> None:
        self._providers: dict[str, _Provider] = {}
        self._frozen = False
        self._lock = RLock()

    @overload
    def register(self, factory: F, *, name: str | None = None, resource: bool = False) -> F: ...

    @overload
    def register(
        self, factory: None = None, *, name: str | None = None, resource: bool = False
    ) -> Callable[[F], F]: ...

    def register(
        self, factory: F | None = None, *, name: str | None = None, resource: bool = False
    ) -> F | Callable[[F], F]:
        """Register a factory directly or as a decorator; return it unchanged."""

        def decorate(func: F) -> F:
            if not callable(func):
                raise TypeError("A registered factory must be callable")
            key = (
                name if name is not None else getattr(func, "__name__", "").removeprefix("_build_")
            )
            _validate_name(key)
            with self._lock:
                if self._frozen:
                    raise FrozenError("Registry is frozen; configure it before the first lookup")
                if key in self._providers:
                    raise ValueError(f"Factory already registered: {key!r}")
                self._providers[key] = _Provider(func, resource)
            return func

        return decorate if factory is None else decorate(factory)

    @property
    def frozen(self) -> bool:
        with self._lock:
            return self._frozen

    def freeze(self) -> None:
        """Explicitly finish registration. Repeated calls are harmless."""
        with self._lock:
            self._frozen = True

    def __contains__(self, name: str) -> bool:
        with self._lock:
            return name in self._providers

    def _find(self, name: str) -> _Provider | None:
        with self._lock:
            return self._providers.get(name)
