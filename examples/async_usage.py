"""Run async operations against a local echo server: python examples/async_usage.py."""

import asyncio
from contextlib import asynccontextmanager

from simpleoc import AsyncContainer, Registry

operations = Registry()


@operations.register(resource=True)
@asynccontextmanager
async def _build_Connection(container):
    host, port = await container["address"]
    reader, writer = await asyncio.open_connection(host, port)
    try:
        yield reader, writer
    finally:
        writer.close()
        await writer.wait_closed()


async def run_operation(app, number):
    async with AsyncContainer(operations, parent=app) as usage:
        reader, writer = await usage["Connection"]
        writer.write(f"operation {number}\n".encode())
        await writer.drain()
        response = (await reader.readline()).decode().strip()
        assert response == f"operation {number}"
        return response


async def main():
    handlers = set()

    async def echo(reader, writer):
        try:
            writer.write(await reader.readline())
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    def connected(reader, writer):
        task = asyncio.create_task(echo(reader, writer))
        handlers.add(task)

    async with await asyncio.start_server(connected, "127.0.0.1", 0) as server:
        address = server.sockets[0].getsockname()[:2]
        async with AsyncContainer(values={"address": address}) as app:
            results = await asyncio.gather(*(run_operation(app, n) for n in range(8)))
        await asyncio.gather(*handlers)
        print("\n".join(results))


if __name__ == "__main__":
    asyncio.run(main())
