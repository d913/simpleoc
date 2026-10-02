"""Synchronous containers with a single lock per parent tree."""

import inspect
from collections.abc import Mapping
from contextlib import ExitStack
from threading import RLock
from types import TracebackType
from typing import Any, Self

from .errors import AsyncFactoryError, CircularDependencyError, ClosedError, FrozenError
from .registry import Registry, _validate_name


class Container:
    """Lazy, thread-safe object storage with owner-based parent fallback.

    Configure values and factories before the first lookup. All containers in
    one parent tree share a reentrant lock, including while factories run.
    """

    def __init__(
        self,
        registry: Registry | None = None,
        *,
        parent: "Container | None" = None,
        values: Mapping[str, Any] | None = None,
        label: str = "",
    ) -> None:
        if parent is not None and not isinstance(parent, Container):
            raise TypeError("Container parent must be a Container")
        self._parent = parent
        self._registry = registry if registry is not None else Registry()
        self._values = dict(values) if values is not None else {}
        for name in self._values:
            _validate_name(name)
        self.label = label
        self._lock: RLock = parent._lock if parent is not None else RLock()
        self._building: list[tuple[Container, str]] = parent._building if parent is not None else []
        self._resources = ExitStack()
        self._started = False
        self._closed = False

    @property
    def parent(self) -> "Container | None":
        return self._parent

    @property
    def registry(self) -> Registry:
        return self._registry

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    def _check_open(self) -> None:
        if self._closed:
            raise ClosedError(f"Container {self.label!r} is closed")

    def set(self, name: str, value: Any) -> Self:
        """Supply a borrowed value before lookup starts; it will not be closed."""
        _validate_name(name)
        with self._lock:
            self._check_open()
            if self._started:
                raise FrozenError("Container values are frozen after the first lookup")
            self._values[name] = value
        return self

    def __setitem__(self, name: str, value: Any) -> None:
        self.set(name, value)

    def __contains__(self, name: str) -> bool:
        """Check availability without constructing objects or freezing configuration."""
        with self._lock:
            self._check_open()
            return (
                name in self._values
                or name in self.registry
                or (self.parent is not None and name in self.parent)
            )

    def __getitem__(self, name: str) -> Any:
        return self.get(name)

    def get(self, name: str, default: Any = None) -> Any:
        """Resolve locally, then from the parent. Missing names return default."""
        _validate_name(name)
        with self._lock:
            self._check_open()
            self._started = True
            self.registry.freeze()
            if name in self._values:
                return self._values[name]
            provider = self.registry._find(name)
            if provider is None:
                return self.parent.get(name, default) if self.parent is not None else default

            node = (self, name)
            if node in self._building:
                path = self._building[self._building.index(node) :] + [node]
                raise CircularDependencyError(
                    " -> ".join(f"{owner.label or 'container'}[{key}]" for owner, key in path)
                )
            self._building.append(node)
            try:
                value = provider.factory(self)
                if inspect.isawaitable(value):
                    if inspect.iscoroutine(value):
                        value.close()
                    raise AsyncFactoryError(f"Factory {name!r} requires AsyncContainer")
                if provider.resource:
                    value = self._resources.enter_context(value)
                self._values[name] = value
                return value
            finally:
                self._building.pop()

    def close(self) -> None:
        """Close owned resources in reverse entry order; never close the parent."""
        with self._lock:
            if self._closed:
                return
            if self._building:
                raise RuntimeError(
                    "Cannot close a container while a factory in its tree is running"
                )
            self._closed = True
            try:
                self._resources.close()
            finally:
                self._values.clear()

    def __enter__(self) -> Self:
        with self._lock:
            self._check_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
