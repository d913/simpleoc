"""Async resolution: one construction task per object, with cycle detection."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping
from contextlib import AsyncExitStack
from contextvars import ContextVar
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from .errors import CircularDependencyError, ClosedError, FrozenError
from .registry import Registry, _Provider, _validate_name

_Node = tuple["AsyncContainer", str]
_active: ContextVar[_Node | None] = ContextVar("simpleoc_build", default=None)


@dataclass
class _Flight:
    result: asyncio.Future[Any]
    task: asyncio.Task[None] | None = None


def _observe_result(result: asyncio.Future[Any]) -> None:
    # A cancelled waiter can leave a failing construction with no remaining waiter.
    if not result.cancelled():
        result.exception()


class _Builds:
    """Construction and wait dependencies shared by one parent tree/event loop."""

    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.flights: dict[_Node, _Flight] = {}
        self.waits: dict[_Node, dict[_Node, int]] = {}

    def bind(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self.loop is None:
            self.loop = loop
        elif self.loop is not loop:
            raise RuntimeError("An AsyncContainer tree belongs to one event loop")
        return loop

    def add_wait(self, source: _Node, target: _Node) -> None:
        # Count edges because a factory may concurrently request the same dependency.
        pending = [(target, [target])]
        seen: set[_Node] = set()
        while pending:
            node, path = pending.pop()
            if node == source:
                cycle = [source, *path]
                raise CircularDependencyError(
                    " -> ".join(f"{c.label or 'container'}[{key}]" for c, key in cycle)
                )
            if node not in seen:
                seen.add(node)
                pending.extend((child, [*path, child]) for child in self.waits.get(node, {}))
        edges = self.waits.setdefault(source, {})
        edges[target] = edges.get(target, 0) + 1

    def remove_wait(self, source: _Node, target: _Node) -> None:
        edges = self.waits[source]
        edges[target] -= 1
        if not edges[target]:
            del edges[target]
        if not edges:
            del self.waits[source]


class AsyncContainer:
    """Lazy container for synchronous or awaitable factories on one event loop.

    Use ``await container.get(name)`` or ``await container[name]``. Cancelling a
    waiter leaves shared construction running. ``aclose()`` cancels this
    container's pending constructions and releases its owned resources.
    """

    def __init__(
        self,
        registry: Registry | None = None,
        *,
        parent: AsyncContainer | None = None,
        values: Mapping[str, Any] | None = None,
        label: str = "",
    ) -> None:
        if parent is not None and not isinstance(parent, AsyncContainer):
            raise TypeError("AsyncContainer parent must be an AsyncContainer")
        self._parent = parent
        self._registry = registry if registry is not None else Registry()
        self._values = dict(values) if values is not None else {}
        for name in self._values:
            _validate_name(name)
        self.label = label
        self._builds: _Builds = parent._builds if parent is not None else _Builds()
        self._resources = AsyncExitStack()
        self._started = False
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    @property
    def parent(self) -> AsyncContainer | None:
        return self._parent

    @property
    def registry(self) -> Registry:
        return self._registry

    @property
    def closed(self) -> bool:
        return self._closed

    def _check_open(self) -> None:
        if self._builds.loop is not None:
            self._builds.bind()
        if self._closed:
            raise ClosedError(f"Container {self.label!r} is closed")

    def set(self, name: str, value: Any) -> Self:
        """Supply a borrowed value before lookup starts; it will not be closed."""
        _validate_name(name)
        self._check_open()
        if self._started:
            raise FrozenError("Container values are frozen after the first lookup")
        self._values[name] = value
        return self

    def __setitem__(self, name: str, value: Any) -> None:
        self.set(name, value)

    def __contains__(self, name: str) -> bool:
        self._check_open()
        return (
            name in self._values
            or name in self.registry
            or (self.parent is not None and name in self.parent)
        )

    async def __getitem__(self, name: str) -> Any:
        return await self.get(name)

    async def get(self, name: str, default: Any = None) -> Any:
        """Resolve once per owning container, awaiting a factory when necessary."""
        _validate_name(name)
        loop = self._builds.bind()
        self._check_open()
        self._started = True
        self.registry.freeze()
        if name in self._values:
            return self._values[name]
        provider = self.registry._find(name)
        if provider is None:
            return await self.parent.get(name, default) if self.parent is not None else default

        node = (self, name)
        source = _active.get()
        # Contexts can outlive their factory when user code starts background tasks.
        if source not in self._builds.flights:
            source = None
        if source is not None:
            self._builds.add_wait(source, node)
        try:
            flight = self._builds.flights.get(node)
            if flight is None:
                result = loop.create_future()
                result.add_done_callback(_observe_result)
                flight = _Flight(result)
                # Publish before starting the task, including with eager task factories.
                self._builds.flights[node] = flight
                flight.task = loop.create_task(self._build(node, provider, result))
                flight.task.add_done_callback(lambda task: self._finish_task(node, flight, task))
            return await asyncio.shield(flight.result)
        finally:
            if source is not None:
                self._builds.remove_wait(source, node)

    def _finish_task(self, node: _Node, flight: _Flight, task: asyncio.Task[None]) -> None:
        # Cancellation before the coroutine's first step cannot run its finally block.
        if not flight.result.done():
            if task.cancelled():
                flight.result.cancel()
            elif (error := task.exception()) is not None:
                flight.result.set_exception(error)
        if self._builds.flights.get(node) is flight:
            self._builds.flights.pop(node)

    async def _build(self, node: _Node, provider: _Provider, result: asyncio.Future[Any]) -> None:
        token = _active.set(node)
        try:
            value = provider.factory(self)
            if inspect.isawaitable(value):
                value = await value
            if provider.resource:
                if hasattr(value, "__aenter__"):
                    value = await self._resources.enter_async_context(value)
                else:
                    value = self._resources.enter_context(value)
            # A factory may catch cancellation; a closing container must not cache it.
            self._check_open()
            self._values[node[1]] = value
            result.set_result(value)
        except asyncio.CancelledError:
            result.cancel()
        except BaseException as exc:
            result.set_exception(exc)
        finally:
            self._builds.flights.pop(node, None)
            _active.reset(token)

    async def aclose(self) -> None:
        """Finish cancellation and cleanup, including when this caller is cancelled."""
        self._builds.bind()
        active = _active.get()
        if active in self._builds.flights:
            raise RuntimeError("A factory cannot close a container in its own tree")
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._finish_close())
        try:
            await asyncio.shield(self._close_task)
        except asyncio.CancelledError:
            await asyncio.shield(self._close_task)
            raise

    async def _finish_close(self) -> None:
        pending = [
            flight.task
            for (owner, _), flight in list(self._builds.flights.items())
            if owner is self and flight.task is not None
        ]
        for task in pending:
            task.cancel()
        try:
            await asyncio.gather(*pending, return_exceptions=True)
            await self._resources.aclose()
        finally:
            self._values.clear()

    async def __aenter__(self) -> Self:
        self._builds.bind()
        self._check_open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()
