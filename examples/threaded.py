"""Run isolated SQLite jobs sharing one app container: python examples/threaded.py."""

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from simpleoc import Container, Registry

jobs = Registry()


@jobs.register(resource=True)
@contextmanager
def _build_Connection(container):
    connection = sqlite3.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


def run_job(app, number):
    with Container(jobs, parent=app, values={"number": number}) as usage:
        connection = usage["Connection"]
        connection.execute("create table result (value integer)")
        connection.execute("insert into result values (?)", (usage["number"],))
        row = connection.execute("select value from result").fetchone()
        assert usage["settings"] is app["settings"]
        return f"{usage['settings']['prefix']}: {row[0]}"


def main():
    with Container(values={"settings": {"prefix": "job"}}) as app:
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda number: run_job(app, number), range(8)))
        assert results == [f"job: {number}" for number in range(8)]
        print("\n".join(results))


if __name__ == "__main__":
    main()
