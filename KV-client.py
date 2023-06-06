import asyncio
from multiprocessing import Process


class Client:

    def __init__(self, host: str, port: int):
        self.is_subscribed = False
        self.server_port: str = None
        self.server_host: int = None
        self.host = host
        self.port = port

    async def start(self):
        print("client started")
        server = await asyncio.start_server(self.serve, self.host, self.port)

        await server.serve_forever()

    async def subscribe(self, server_host: str, server_port: int):
        self.server_host = server_host
        self.server_port = server_port
        response = await self._send_request_unsafe(f"subscribe {self.port}")
        if response == b"OK\n":
            self.is_subscribed = True
            print("subscribed")

    async def serve(self, reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter):
        print(await reader.readline())

    async def send_request(self, request):
        if not self.is_subscribed:
            return

        return await self._send_request_unsafe(request)

    async def _send_request_unsafe(self, request):
        return await send_request(self.server_host, self.server_port, request)


async def send_request(host, port, request):
    reader, writer = await asyncio.open_connection(host, port,
                                                   local_addr=(host, 0))
    print("Connected")
    writer.write(bytes(request + "\n", encoding="utf8"))
    await writer.drain()
    res = await reader.readline()
    print(res)
    writer.close()
    await writer.wait_closed()
    return res


async def start(host, port):
    client = Client(host, port + 1)
    asyncio.create_task(client.start())
    while True:
        cmd = input()
        await asyncio.create_task(client.send_request(cmd))

    await wait_all()


async def wait_all():
    tasks = asyncio.all_tasks()
    cur_task = asyncio.current_task()
    tasks.remove(cur_task)
    await asyncio.wait(tasks)


if __name__ == "__main__":
    host, port = "127.0.0.1", 8000

    asyncio.run(start(host, port))

