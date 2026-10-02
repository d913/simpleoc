# simpleoc

Lazy object containers with decorator-driven registries and single-parent lookup.
Python 3.11 or newer. No runtime dependencies.

A **registry** holds factories. A **container** owns instances and may refer to
one parent. Each factory receives the container that owns its registration, and
its result is cached there. A child can use parent objects; it cannot change how
the parent builds them.

## Installation

From a checkout:

```bash
python -m pip install .
```

The intended distribution and import name is `simpleoc`. This checkout does not
constitute a PyPI release; after publication, install with `pip install simpleoc`.

## Start with a container

Containers are ready to use immediately after construction. `with` is optional:
it arranges cleanup when a block exits, rather than starting or activating the
container. Creation, use, and cleanup can happen in different functions or files.

```python
from simpleoc import Container

app = Container(label="app")
app["greeting"] = "Hello"


@app.registry.register
def _build_Message(container):
    return {"text": container["greeting"]}


# Nothing is built until requested. The _build_ prefix is removed.
first = app["Message"]
assert first is app["Message"]
assert first == {"text": "Hello"}

# Call this when the application shuts down, after its operations finish.
app.close()
```

Use `container.set(name, value)` or assignment for existing values. These values
are borrowed: the container will not close them. Callable values assigned this
way remain callable values; they are not invoked as factories.

Configure values and factories **before the first lookup**. Entering `with`
does not perform a lookup. Once lookup starts, changing local values raises
`FrozenError`, and the attached registry freezes. Finish setup before handing
containers to other threads or tasks.

## Reuse a registry

```python
from simpleoc import Container, Registry

registry = Registry()


@registry.register
def _build_Items(container):
    return []


@registry.register(name="description")
def describe(container):
    return "Items for one operation"


with Container(registry) as first, Container(registry) as second:
    first["Items"].append("one")
    assert second["Items"] == []
```

`registry.register(factory, name="key")` also works without decorator syntax.
The decorator returns the original function. Inferred function names lose a
leading `_build_`; explicit names are used exactly as supplied. Names must be
nonempty strings. Duplicate registration raises `ValueError`.

Registries contain recipes, not instances. Each attached container has its own
cache. The first lookup in **any** attached container freezes the shared registry;
`registry.freeze()` can finish registration earlier. `registry.frozen` reports
its state. Build a separate registry when you need different registrations.

## App and usage lifetimes

Retain an app container for the application's lifetime. Make one child per
request, job, or other operation, passing that child explicitly to the operation.

```python
from simpleoc import Container, Registry

app_registry = Registry()
usage_registry = Registry()


@app_registry.register
def _build_Settings(container):
    return {"region": "local"}


@usage_registry.register
def _build_Request(container):
    return {
        "id": container["request_id"],
        "settings": container["Settings"],
    }


with Container(app_registry, label="app") as app:
    with Container(usage_registry, parent=app, values={"request_id": "one"}) as one:
        with Container(usage_registry, parent=app, values={"request_id": "two"}) as two:
            assert one["Request"] is not two["Request"]
            assert one["Settings"] is two["Settings"] is app["Settings"]
```

Children of the same app container share its app objects. Separate app containers
own independent caches. Pass the intended container explicitly to each operation.

The child references its parent; the parent does not keep a list of children.
Finish operations and close children before closing their parent. Containers
and values become collectible when no remaining references retain them; finishing
a thread or task does not by itself guarantee resource cleanup.

## Containers created and used in different files

An app container can be created at startup and passed to code that creates
children later. Neither container needs a `with` block. The application lifecycle
owns app shutdown; each operation owns cleanup of its child.

These four files separate registration, app lifecycle, operation code, and
startup. The registry module defines factories; the lifecycle module creates
the container that will own their instances.

```python
# app_registry.py
from simpleoc import Registry

registry = Registry()


@registry.register
def _build_Settings(container):
    return {"region": "local"}
```

```python
# app_lifecycle.py
from simpleoc import Container
from app_registry import registry


def create_app():
    return Container(registry, label="app")


def shutdown_app(app):
    app.close()
```

```python
# operations.py
from simpleoc import Container


def handle_request(app, request_id):
    usage = Container(parent=app, values={"request_id": request_id})
    try:
        return {
            "id": usage["request_id"],
            "region": usage["Settings"]["region"],
        }
    finally:
        usage.close()  # The app container stays open.
```

```python
# bootstrap.py
from app_lifecycle import create_app, shutdown_app
from operations import handle_request

app = create_app()
try:
    assert handle_request(app, "one") == {"id": "one", "region": "local"}
    assert handle_request(app, "two") == {"id": "two", "region": "local"}
finally:
    shutdown_app(app)
```

In a framework, the same calls can live in startup, request/job teardown, and
shutdown callbacks. Retain the app container in the application's state and pass
it to operation code. If an operation's setup and teardown also live in separate
callbacks, retain its usage container in that operation's state until teardown.
Use `finally` or the framework's guaranteed teardown mechanism so failures still
trigger cleanup. Let active operations finish before closing the app.

`with` remains useful for a local lifetime, and you can mix styles: an explicitly
managed app container can be the parent of a child managed with `with` elsewhere.
Lexical nesting of blocks has no role in parent lookup or object ownership.

## Exact lookup and override rules

Lookup checks local supplied/cached values, then the local registry, then the
parent, recursively. Successful construction, including `None`, is cached in
the owning container. Missing names return `None`; `get(name, default)` supplies
a different missing-name result. A factory's own exception propagates unchanged.

`name in container` checks availability without building or freezing anything.
It distinguishes a missing name from a registered/cached `None`.

```python
from simpleoc import Container

a = Container(values={"item": "a"})
b = Container(parent=a)
b.registry.register(lambda owner: owner["item"], name="dep")
c = Container(parent=b, values={"item": "c"})

assert c["item"] == "c"
assert c["dep"] == "a"  # b owns dep; resolution starts at b, not c.
c.close()
b.close()
a.close()
```

To build a child-specific `dep`, register its factory in that child as well.
To wrap an ancestor implementation with the same name, explicitly use
`container.parent["name"]` inside the child's factory (after establishing that
a parent exists). Parents are supplied at construction and are read-only.

Factories should resolve dependencies through the container they receive.
Dependencies on captured containers, global state, or ambient request context
are outside this ownership model.

## Threads

`Container` protects lookup, construction, configuration, and cleanup with a
reentrant lock shared by its parent tree. Concurrent requests for the same
object receive one successfully constructed instance. Nested lookups work;
dependency cycles raise `CircularDependencyError` with a dependency path.

Construction is deliberately serialized across the tree, including different
keys. Keep factories reasonably short. A factory must not start another thread
and wait for that thread to resolve from the same tree: the factory holds the
tree's lock. Resolving into unrelated trees through captured references can
also introduce lock-order deadlocks and is outside the supported model.

Failed construction is not cached. Waiting callers can retry after the failure.
Synchronization protects container construction, **not methods on the objects
it returns**. Shared objects must themselves support concurrent use.

See `examples/threaded.py` for independent SQLite connections per job sharing
one app configuration.

## Async factories and tasks

Use `AsyncContainer` throughout a parent tree that needs awaitable factories.
It accepts both ordinary and async factories. Resolve with
`await container.get(name)` or `await container[name]`.

```python
import asyncio
from simpleoc import AsyncContainer, Registry

registry = Registry()


@registry.register
async def _build_Item(container):
    await asyncio.sleep(0)  # Replace with actual async initialization.
    return object()


async def use_app(app):
    usage = AsyncContainer(parent=app)
    try:
        first, second = await asyncio.gather(usage.get("Item"), app.get("Item"))
        assert first is second
    finally:
        await usage.aclose()


async def main():
    app = AsyncContainer(registry)
    try:
        await use_app(app)  # This function can be defined in another module.
    finally:
        await app.aclose()


asyncio.run(main())
```

`async with` is optional too. Creation, use, and `await aclose()` may happen in
separate lifecycle callbacks, provided async use and cleanup share the same
event loop. There is no explicit start/open call.

Each async tree belongs to one event loop, bound on first async use. It cannot
be reused across separate `asyncio.run()` calls or shared between event-loop
threads. Create separate trees there. Synchronous and async container types
cannot be mixed in one parent chain. A synchronous container receiving an
awaitable factory result raises `AsyncFactoryError`.

Async behavior:

- Concurrent requests for one object share its construction; different objects
  can build concurrently. Dependency cycles, including cycles between independently
  started factories, raise `CircularDependencyError`.
- Cancelling a waiting caller does not cancel shared construction. The object
  remains available to other callers and stays owned by its container.
- Factory failure reaches current waiters; a later lookup retries. Factory
  cancellation cancels current waiters and also permits a later retry.
- `aclose()` rejects new lookup, cancels and awaits this container's pending
  constructions, then releases its resources. Parent constructions are left alone.
- Cancellation while awaiting `aclose()` is re-raised after cleanup completes.
  Repeated external cancellation or stopping the event loop can interrupt the
  wait; keep the loop running until shutdown finishes. Factories must cooperate
  with cancellation for shutdown to complete.

Make one child per async **operation**, not per thread. Child tasks may share
that child if they intentionally share its instances. Await those tasks before
leaving the container's context. Ordinary factories run on the event-loop thread;
they must not perform blocking I/O there. Factories should await their dependency
lookups and keep any tasks they spawn within their own lifetime.

See `examples/async_usage.py` for an executable local TCP example.

## Resource ownership and cleanup

Opt into ownership with `resource=True`. The factory must return a context
manager; the cached value is the value returned by its `__enter__`/`__aenter__`.

```python
from contextlib import contextmanager
import sqlite3
from simpleoc import Container, Registry

registry = Registry()


@registry.register(resource=True)
@contextmanager
def _build_Connection(container):
    connection = sqlite3.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


with Container(registry) as usage:
    assert usage["Connection"].execute("select 1").fetchone() == (1,)
```

Place `@registry.register(resource=True)` **above** `@contextmanager` or
`@asynccontextmanager`. `AsyncContainer` accepts both kinds; an async factory can
also return a context manager after awaiting initialization.

Resources exit in reverse entry order, keeping dependencies available until
their dependents close. Resources belonging to a parent stay with that parent.
Ordinary factory results and supplied values are never automatically closed.
Lazy resources that were never requested are never opened.

Use `with`/`async with`, or call `close()`/`await aclose()` explicitly from a
`finally` block or guaranteed teardown callback. These patterns ensure cleanup
runs even when the operation raises; merely constructing a container does not
register an automatic shutdown hook. Resource exits receive normal-close
arguments, not the operation's exception: these contexts manage lifetime, not
transaction commit/rollback. Cleanup attempts the remaining exits if one fails;
errors propagate. Cleanup is attempted once; repeated async close awaits the
same result, including any error. Use-after-close raises `ClosedError`.

If construction fails after resolving dependencies, those successful dependencies
remain cached/owned until their container closes. No automatic transaction or
rollback of the dependency graph occurs.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python -m pytest
ruff check .
ruff format --check .
mypy
python examples/threaded.py
python examples/async_usage.py
```

The layout is `src/simpleoc` for the package, `tests` for behavioral tests, and
`examples` for executable integrations. CI runs tests on Python 3.11–3.14 and
checks distribution metadata. The package includes `py.typed`.

## Build and publish

Packaging uses the standard [`pyproject.toml`](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)
format and Hatchling.

```bash
python -m build
python -m twine check --strict dist/*
```

Before publishing, confirm the PyPI project name/account, add your author and
repository/support URLs to `[project]`, and choose the release version there.
The starting version is `0.1.0`. Test the wheel and source distribution in clean
environments. For later releases, build into a clean `dist` directory so an
upload does not include old artifacts.

When ready, upload explicitly, using your own credentials or trusted publishing:

```bash
python -m twine upload --repository testpypi dist/*
# After verifying the TestPyPI release:
python -m twine upload dist/*
```

No publication credentials or automatic release workflow are included.

## License

[BSD Zero Clause (0BSD)](https://spdx.org/licenses/0BSD.html). Use, modify, and
redistribute for any purpose, including commercial use. See `LICENSE`.
