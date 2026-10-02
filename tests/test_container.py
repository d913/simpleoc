import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Barrier

import pytest

from simpleoc import (
    AsyncFactoryError,
    CircularDependencyError,
    ClosedError,
    Container,
    FrozenError,
    Registry,
)


def test_direct_container_decorator_is_lazy_and_returns_original():
    container = Container()
    calls = []

    @container.registry.register
    def _build_Item(owner):
        calls.append(owner)
        return object()

    assert callable(_build_Item)
    assert "Item" in container
    assert calls == []
    assert container["Item"] is container.get("Item")
    assert calls == [container]


def test_registration_forms_and_exact_explicit_name():
    registry = Registry()

    @registry.register()
    def inferred(c):
        return 1

    @registry.register(name="_build_explicit")
    def other(c):
        return 2

    def direct(c):
        return 3

    assert registry.register(direct, name="third") is direct
    c = Container(registry)
    assert [c[key] for key in ("inferred", "_build_explicit", "third")] == [1, 2, 3]


def test_duplicate_and_invalid_registration():
    r = Registry()
    r.register(lambda c: None, name="x")
    with pytest.raises(ValueError, match="already registered"):
        r.register(lambda c: None, name="x")
    with pytest.raises(ValueError, match="nonempty"):
        r.register(lambda c: None, name="")
    with pytest.raises(TypeError, match="callable"):
        r.register(42)


def test_registry_and_values_freeze_at_first_lookup():
    r = Registry()
    a, b = Container(r), Container(r)
    assert "missing" not in a
    assert not r.frozen
    a["given"] = 1
    assert a["given"] == 1
    with pytest.raises(FrozenError):
        r.register(lambda c: 2, name="new")
    with pytest.raises(FrozenError):
        a["given"] = 3
    b["given"] = 4
    assert b["given"] == 4


def test_shared_registry_independent_caches_and_application_instances():
    r = Registry()
    r.register(lambda c: object(), name="item")
    app1, app2 = Container(r), Container(r)
    assert app1["item"] is not app2["item"]
    assert Container(parent=app1)["item"] is app1["item"]


@pytest.mark.parametrize("warm", [True, False])
def test_usage_caches_and_app_identity_do_not_depend_on_warmup(warm):
    app_registry, usage_registry = Registry(), Registry()
    app_registry.register(lambda c: object(), name="app")
    usage_registry.register(lambda c: object(), name="usage")
    app = Container(app_registry)
    if warm:
        app["app"]
    u1 = Container(usage_registry, parent=app)
    u2 = Container(usage_registry, parent=app)
    assert u1["app"] is u2["app"] is app["app"]
    assert u1["usage"] is not u2["usage"]


def test_owner_resolution_ignores_descendant_overrides():
    a = Container(values={"item": "a"})
    b = Container(parent=a)
    b.registry.register(lambda owner: owner["item"], name="dep")
    c = Container(parent=b, values={"item": "c"})
    assert c["item"] == "c"
    assert c["dep"] == b["dep"] == "a"


def test_child_can_register_factory_to_own_overridden_dependency():
    r = Registry()
    r.register(lambda c: c["item"], name="dep")
    parent = Container(r, values={"item": "parent"})
    child = Container(r, parent=parent, values={"item": "child"})
    assert child["dep"] == "child"
    assert parent["dep"] == "parent"


def test_explicit_parent_wrapping():
    parent = Container(values={"service": ["parent"]})
    child = Container(parent=parent)
    child.registry.register(lambda c: ["child", *c.parent["service"]], name="service")
    assert child["service"] == ["child", "parent"]


def test_false_values_missing_default_and_callable_values():
    def fn():
        return 42

    c = Container(values={"none": None, "false": False, "zero": 0, "callable": fn})
    c.registry.register(lambda c: None, name="built_none")
    assert c.get("none", "fallback") is None
    assert c["false"] is False
    assert c["zero"] == 0
    assert c["callable"] is fn
    assert c["missing"] is None
    assert c.get("missing", "fallback") == "fallback"
    assert c["built_none"] is None
    assert "built_none" in c
    assert "missing" not in c


def test_parent_is_immutable():
    c = Container()
    with pytest.raises(AttributeError):
        c.parent = c
    with pytest.raises(TypeError):
        Container(parent=object())


def test_concurrent_first_lookup_builds_once_across_children():
    app = Container()
    calls = []
    start = Barrier(8)

    @app.registry.register(name="item")
    def build(c):
        calls.append(c)
        time.sleep(0.02)
        return object()

    def lookup(_):
        child = Container(parent=app)
        start.wait(timeout=5)
        return child["item"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        values = list(pool.map(lookup, range(8)))
    assert all(value is values[0] for value in values)
    assert calls == [app]


def test_nested_build_and_cycle_detection():
    c = Container(label="app")
    c.registry.register(lambda c: c["b"], name="a")
    c.registry.register(lambda c: c["a"], name="b")
    with pytest.raises(CircularDependencyError, match=r"app\[a\].*app\[b\].*app\[a\]"):
        c["a"]
    with pytest.raises(CircularDependencyError):
        c["a"]  # A failed attempt leaves no stale in-progress state.


def test_failed_factory_retries():
    c = Container()
    calls = []

    @c.registry.register(name="item")
    def build(c):
        calls.append(1)
        if len(calls) == 1:
            raise LookupError("temporary")
        return object()

    with pytest.raises(LookupError, match="temporary"):
        c["item"]
    assert c["item"] is c["item"]
    assert len(calls) == 2


def test_sync_rejects_async_factory_without_unawaited_coroutine():
    c = Container()

    @c.registry.register
    async def item(owner):
        return object()

    with pytest.raises(AsyncFactoryError):
        c["item"]


def test_resources_close_in_dependency_order_and_leave_parent_open():
    events = []
    app = Container()
    usage = Registry()

    @app.registry.register(resource=True)
    @contextmanager
    def connection(c):
        events.append("open connection")
        try:
            yield object()
        finally:
            events.append("close connection")

    @usage.register(resource=True)
    @contextmanager
    def session(c):
        c["connection"]
        events.append("open session")
        try:
            yield object()
        finally:
            events.append("close session")

    with app:
        with pytest.raises(ValueError), Container(usage, parent=app) as child:
            child["session"]
            raise ValueError("operation failed")
        assert events == ["open connection", "open session", "close session"]
        assert not app.closed
    assert events[-1] == "close connection"
    app.close()


def test_cleanup_continues_after_exit_failure_and_closes_once():
    events = []
    c = Container()

    @c.registry.register(resource=True)
    @contextmanager
    def a(c):
        try:
            yield object()
        finally:
            events.append("a")

    @c.registry.register(resource=True)
    @contextmanager
    def b(c):
        c["a"]
        try:
            yield object()
        finally:
            events.append("b")
            raise ValueError("cleanup failed")

    c["b"]
    with pytest.raises(ValueError, match="cleanup failed"):
        c.close()
    assert events == ["b", "a"]
    c.close()
    with pytest.raises(ClosedError):
        c["a"]
    with pytest.raises(ClosedError):
        c.set("x", 1)


def test_borrowed_values_are_not_automatically_closed():
    class Borrowed:
        def close(self):
            pytest.fail("borrowed resource closed")

    with Container(values={"borrowed": Borrowed()}) as c:
        assert isinstance(c["borrowed"], Borrowed)


def test_factory_cannot_close_its_container():
    c = Container()
    c.registry.register(lambda c: c.close(), name="bad")
    with pytest.raises(RuntimeError, match="factory"):
        c["bad"]
    assert not c.closed
