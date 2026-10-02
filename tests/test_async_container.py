import asyncio
from contextlib import asynccontextmanager, contextmanager

import pytest

from simpleoc import AsyncContainer, CircularDependencyError, ClosedError, FrozenError, Registry


async def test_sync_and_async_factories_are_lazy_cached_and_owner_resolved():
    app = AsyncContainer(values={"item": "app"})
    calls = []

    @app.registry.register
    async def dep(c):
        calls.append(c)
        return await c["item"]

    @app.registry.register
    def sync_value(c):
        return object()

    child = AsyncContainer(parent=app, values={"item": "child"})
    assert calls == []
    async with app, child:
        assert await child["item"] == "child"
        assert await child["dep"] == "app"
        assert await app["dep"] == "app"
        assert await child["sync_value"] is await app["sync_value"]
        assert calls == [app]


async def test_registry_reuse_isolated_usage_and_shared_app():
    app_registry, usage_registry = Registry(), Registry()
    app_registry.register(lambda c: object(), name="app")
    usage_registry.register(lambda c: object(), name="usage")
    async with AsyncContainer(app_registry) as app:
        async with AsyncContainer(usage_registry, parent=app) as a:
            async with AsyncContainer(usage_registry, parent=app) as b:
                assert await a["app"] is await b["app"] is await app["app"]
                assert await a["usage"] is not await b["usage"]


async def test_concurrent_first_access_and_independent_factories():
    c = AsyncContainer()
    started = asyncio.Event()
    release = asyncio.Event()
    calls = []

    @c.registry.register
    async def item(c):
        calls.append(1)
        started.set()
        await release.wait()
        return object()

    c.registry.register(lambda c: "independent", name="other")
    async with c:
        tasks = [asyncio.create_task(c.get("item")) for _ in range(10)]
        await started.wait()
        assert await asyncio.wait_for(c.get("other"), 1) == "independent"
        release.set()
        values = await asyncio.gather(*tasks)
        assert all(v is values[0] for v in values)
        assert len(calls) == 1


async def test_cancelled_waiter_does_not_cancel_shared_construction():
    c = AsyncContainer()
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    @c.registry.register
    async def item(c):
        calls.append(1)
        started.set()
        await release.wait()
        return object()

    async with c:
        first = asyncio.create_task(c.get("item"))
        await started.wait()
        second = asyncio.create_task(c.get("item"))
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        assert await second is await c["item"]
        assert calls == [1]


async def test_failed_construction_releases_all_waiters_and_retries():
    c = AsyncContainer()
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    @c.registry.register
    async def item(c):
        calls.append(1)
        if len(calls) == 1:
            started.set()
            await release.wait()
            raise ValueError("temporary")
        return object()

    async with c:
        first = asyncio.create_task(c.get("item"))
        await started.wait()
        second = asyncio.create_task(c.get("item"))
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        assert all(isinstance(result, ValueError) for result in results)
        assert await c["item"] is await c["item"]
        assert len(calls) == 2


async def test_direct_dependency_cycle():
    c = AsyncContainer(label="app")
    c.registry.register(lambda c: c.get("b"), name="a")
    c.registry.register(lambda c: c.get("a"), name="b")
    async with c:
        with pytest.raises(CircularDependencyError, match="app"):
            await asyncio.wait_for(c.get("a"), 1)


async def test_cycle_between_independently_started_factories():
    c = AsyncContainer()
    a_started, b_started = asyncio.Event(), asyncio.Event()

    @c.registry.register
    async def a(c):
        a_started.set()
        await b_started.wait()
        return await c["b"]

    @c.registry.register
    async def b(c):
        b_started.set()
        await a_started.wait()
        return await c["a"]

    async with c:
        results = await asyncio.wait_for(
            asyncio.gather(c.get("a"), c.get("b"), return_exceptions=True), 1
        )
        assert all(isinstance(r, CircularDependencyError) for r in results)


async def test_concurrent_duplicate_dependency_requests_are_not_cycles():
    c = AsyncContainer()

    @c.registry.register
    async def dep(c):
        await asyncio.sleep(0)
        return object()

    @c.registry.register
    async def item(c):
        return await asyncio.gather(c.get("dep"), c.get("dep"))

    async with c:
        a, b = await c["item"]
        assert a is b


async def test_async_and_sync_resources_cleanup_in_reverse_order():
    events = []
    c = AsyncContainer()

    @c.registry.register(resource=True)
    @contextmanager
    def base(c):
        try:
            yield object()
        finally:
            events.append("base")

    @c.registry.register(resource=True)
    @asynccontextmanager
    async def service(c):
        await c["base"]
        try:
            yield object()
        finally:
            await asyncio.sleep(0)
            events.append("service")

    with pytest.raises(ValueError):
        async with c:
            await c["service"]
            raise ValueError("operation")
    assert events == ["service", "base"]
    await c.aclose()
    with pytest.raises(ClosedError):
        await c["service"]


async def test_close_cancels_pending_build_and_leaves_parent_open():
    parent = AsyncContainer(values={"app": object()})
    child = AsyncContainer(parent=parent)
    started = asyncio.Event()
    cancelled = []

    @child.registry.register
    async def pending(c):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    async with parent:
        task = asyncio.create_task(child.get("pending"))
        await started.wait()
        await child.aclose()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled == [True]
        assert await parent["app"] is not None
        assert not parent.closed


async def test_close_before_build_task_starts_unblocks_waiter():
    c = AsyncContainer()
    c.registry.register(lambda c: object(), name="item")
    waiter = asyncio.create_task(c.get("item"))
    # This schedules the build behind this test's next step.
    await asyncio.sleep(0)
    await c.aclose()
    result = await asyncio.wait_for(asyncio.gather(waiter, return_exceptions=True), 1)
    assert len(result) == 1
    assert c.closed


async def test_cancelled_close_waits_for_cleanup():
    c = AsyncContainer()
    exiting, release = asyncio.Event(), asyncio.Event()
    events = []

    @c.registry.register(resource=True)
    @asynccontextmanager
    async def resource(c):
        try:
            yield object()
        finally:
            exiting.set()
            await release.wait()
            events.append("closed")

    await c["resource"]
    closing = asyncio.create_task(c.aclose())
    await exiting.wait()
    closing.cancel()
    await asyncio.sleep(0)
    assert not closing.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert events == ["closed"]


async def test_freeze_missing_none_and_parent_immutability():
    async with AsyncContainer(values={"none": None}) as c:
        assert "none" in c
        assert "missing" not in c
        assert await c.get("none", 42) is None
        assert await c.get("missing", 42) == 42
        assert await c["missing"] is None
        with pytest.raises(FrozenError):
            c["new"] = 1
        with pytest.raises(FrozenError):
            c.registry.register(lambda c: 1, name="new")
        with pytest.raises(AttributeError):
            c.parent = c


def test_async_tree_rejects_second_event_loop():
    c = AsyncContainer(values={"x": 1})
    assert asyncio.run(c.get("x")) == 1
    with pytest.raises(RuntimeError, match="event loop"):
        asyncio.run(c.get("x"))


async def test_factory_cannot_close_its_tree():
    c = AsyncContainer()
    c.registry.register(lambda c: c.aclose(), name="bad")
    async with c:
        with pytest.raises(RuntimeError, match="factory"):
            await c["bad"]


async def test_cancelled_factory_can_be_retried():
    calls = []
    c = AsyncContainer()

    @c.registry.register
    async def item(c):
        calls.append(1)
        if len(calls) == 1:
            raise asyncio.CancelledError
        return object()

    async with c:
        with pytest.raises(asyncio.CancelledError):
            await c["item"]
        assert await c["item"] is await c["item"]
        assert calls == [1, 1]


async def test_child_close_does_not_cancel_parent_construction():
    started, release = asyncio.Event(), asyncio.Event()
    parent = AsyncContainer()
    child = AsyncContainer(parent=parent)

    @parent.registry.register
    async def app(c):
        started.set()
        await release.wait()
        return object()

    child.registry.register(lambda c: c.get("app"), name="usage")
    async with parent:
        task = asyncio.create_task(child.get("usage"))
        await started.wait()
        await child.aclose()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        assert await parent["app"] is await parent["app"]


async def test_abandoned_failing_build_has_no_unhandled_exception():
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    errors = []
    loop.set_exception_handler(lambda loop, context: errors.append(context))
    try:
        started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async with AsyncContainer() as c:

            @c.registry.register
            async def item(c):
                started.set()
                await release.wait()
                finished.set()
                raise ValueError("nobody is waiting")

            task = asyncio.create_task(c.get("item"))
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            await finished.wait()
            await asyncio.sleep(0)
        assert errors == []
    finally:
        loop.set_exception_handler(previous)


async def test_async_cleanup_attempts_all_resources_after_failure():
    events = []
    c = AsyncContainer()

    @c.registry.register(resource=True)
    @asynccontextmanager
    async def base(c):
        try:
            yield object()
        finally:
            events.append("base")

    @c.registry.register(resource=True)
    @asynccontextmanager
    async def child(c):
        await c["base"]
        try:
            yield object()
        finally:
            events.append("child")
            raise ValueError("exit failed")

    await c["child"]
    with pytest.raises(ValueError, match="exit failed"):
        await c.aclose()
    assert events == ["child", "base"]
    assert c.closed


@pytest.mark.skipif(not hasattr(asyncio, "eager_task_factory"), reason="Python >=3.12")
async def test_eager_task_factory():
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        async with AsyncContainer() as c:
            c.registry.register(lambda c: object(), name="value")
            assert await c["value"] is await c["value"]
    finally:
        loop.set_task_factory(previous)
