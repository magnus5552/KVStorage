import sys
import os
import asyncio

class Client:

    def __init__(self, host: str, port: int):
        self.server: asyncio.Server = None
        self.data: dict = {}
        self.is_subscribed = False
        self.server_port: str = None
        self.server_host: int = None
        self.host = host
        self.port = port

    async def start(self):
        print("Client started")
        self.server = await asyncio.start_server(self.serve,
                                                 self.host, self.port)

        await self.server.serve_forever()

    async def subscribe(self, server_host: str, server_port: int):
        self.server_host = server_host
        self.server_port = server_port
        response = await self._send_request_unsafe(f"subscribe {self.port}")
        if response == b"OK\n":
            self.is_subscribed = True
            print("Subscribed")

    async def serve(self, reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter):
        request = (await reader.readline())[:-1].decode()
        print(request)

        if len(request) == 0:
            writer.close()
            return

        cmd, *args = request.split()

        match cmd:
            case "write":
                self.data[args[0]] = args[1]
            case "get":
                writer.write(to_bytes(self.data[args[0]]))
                await writer.drain()
            case "capacity":
                print(to_bytes(str(len(self.data))))
                writer.write(to_bytes(str(len(self.data))))
                await writer.drain()
            case "send" if args[0] == "stop":
                self.server.close()
                await self.server.wait_closed()
            case "send":
                response = await self.send_request(" ".join(args))
                writer.write(response)
                await writer.drain()
            case _:
                print(f"Unknown command: {request}")

        writer.close()

    async def send_request(self, request):
        cmd, *args = request.split()
        if cmd == "subscribe":
            await self.subscribe(args[0], int(args[1]))
            return b"OK" if self.is_subscribed else b"ERROR"

        if not self.is_subscribed:
            return b"You are not subscribed. Subscribe a storage to send requests"

        return await self._send_request_unsafe(request)

    async def _send_request_unsafe(self, request) -> bytes:
        return await send_request(self.server_host, self.server_port, request)


async def send_request(host, port, request) -> bytes:
    reader, writer = await asyncio.open_connection(host, port,
                                                   local_addr=(host, 0))
    writer.write(to_bytes(request))
    await writer.drain()
    res = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return res


def to_bytes(data: str):
    return bytes(data + "\n", encoding="utf8")


async def wait_all():
    tasks = asyncio.all_tasks()
    cur_task = asyncio.current_task()
    tasks.remove(cur_task)
    await asyncio.wait(tasks)


async def input_loop(client: Client):
    await client.send_request("subscribe 127.0.0.1 8000")
    while True:
        request = input()
        response = await client.send_request(request)
        print(response)


async def main():
    client = Client("localhost", 8001)
    await client.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except asyncio.CancelledError:
        pass
