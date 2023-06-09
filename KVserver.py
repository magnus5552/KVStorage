import asyncio
import functools
import inspect
import logging
import os.path
import pathlib
import queue
from collections import defaultdict
from datetime import datetime
from typing import Callable, Awaitable, DefaultDict
import aiofiles
from KVstorage import to_bytes


def init_logger():
    path = pathlib.Path(os.path.join(os.getcwd(), "log"))
    path.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("Server")
    logger.setLevel(logging.DEBUG)

    time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    sh = logging.StreamHandler()
    fh = logging.FileHandler(filename=f'log/{time}_KVserver.log')

    formatter = logging.Formatter(
        '[%(asctime)s] - %(levelname)s - %(message)s')

    sh.setFormatter(formatter)
    fh.setFormatter(formatter)
    logger.addHandler(sh)
    logger.addHandler(fh)

    return logger


def check_args(func, skip=1):
    args_count = len(inspect.signature(func).parameters) - 1

    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if len(args) != args_count:
            signature = " ".join(args[skip:])
            self.logger.warning(f"wrong request signature: {signature}")
            return to_bytes(f"Wrong request signature: {signature}")

        return await func(self, *args, *kwargs)

    return wrapper


def subscribtion_required(func):
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        node = args[0] if len(args) > 0 and isinstance(args[0], KVNode) \
            else None

        if node is not None and node.host not in self.nodes.keys():
            self.logger.warning(f"{node.host} not subscribed")
            return b"Client not subscribed. Subscribe to write data."

        return await func(self, *args, **kwargs)

    return wrapper


class KVNode:

    def __init__(self, host, port, reader: asyncio.StreamReader = None,
                 writer: asyncio.StreamWriter = None):
        self._reader: reader = reader
        self._writer: writer = writer
        self.host = host
        self.port = port

    @property
    def ip(self):
        return f"{self.host}:{self.port}"

    @property
    def is_connected(self) -> bool:
        if self._writer is None:
            return False

        port = self._writer.get_extra_info("peername")[1]
        if port != self.port:
            return False

        return not self._writer.is_closing()

    async def connect(self):
        if self.is_connected:
            return

        self._reader, self._writer = await asyncio.open_connection(self.host,
                                                                   self.port)

    def disconnect(self):
        self._writer.close()

    async def write(self, data: str):
        await self.connect()

        self._writer.write(to_bytes(data))
        await self._writer.drain()

    async def write_data(self, key: str, value: str):
        await self.write(f"write {key} {value}")

    async def get_capacity(self):
        await self.write("capacity")

        response = await self._reader.readline()
        return int(response.decode())

    def __eq__(self, other):
        return other is KVNode and other.host == self.host


class Server:

    def __init__(self, host: str, port: int):
        # словарь ключ - ip для получения нужной ноды по ключу
        self.keys_map: dict[str, str] = {}
        self.nodes: dict[str, KVNode] = {}
        self.nodes_capacities: queue.PriorityQueue[tuple[int, str]] \
            = queue.PriorityQueue()
        self.host: str = host
        self.port: int = port
        self.logger: logging.Logger = init_logger()
        # Словарь команд для сервера. Каждая команда может обрабатываться
        # несколькими функциями, поэтому по ключу хранятся функции
        self.commands: DefaultDict[str, list[Callable[[KVNode, ...],
                                   Awaitable[bytes]]]] = \
            defaultdict(lambda: [], {
                "write": [self.write_data],
                "get": [self.get_data],
                "subscribe": [self.subscribe_node]
            })

    async def start_server(self):
        server = await asyncio.start_server(self.serve_client, self.host,
                                            self.port)
        self.file = await aiofiles.open("server_data", "w")

        try:
            self.logger.info(f"server started on {self.host}:{self.port}")
            await server.serve_forever()
        except Exception as e:
            self.logger.error(e)

    async def serve_client(self,
                           reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter):
        peername = writer.get_extra_info("peername")

        host, port = peername[0], peername[1]
        ip = f"{host}:{port}"
        self.logger.info(f"connected {ip}")

        request = await reader.readline()

        if request is None:
            self.logger.warning(f"{ip} unexpectedly disconnected")
            writer.close()
            return

        node = self.nodes[host] if host in self.nodes.keys() \
            else KVNode(host, port, reader=reader, writer=writer)

        response = await self.handle_request(request, node)

        await self.send_response(writer, response)
        self.logger.info(f"client {ip} served")

    async def send_response(self,
                            writer: asyncio.StreamWriter,
                            response: bytes):
        writer.write(response + b"\n")
        await writer.drain()
        writer.close()

    async def handle_request(self, request: bytes, node: KVNode) -> bytes:
        req_split = request.decode().split()
        command = req_split[0]

        if command not in self.commands.keys():
            self.logger.error("Unknown command")
            return to_bytes(f"Unknown command: {command}")

        responses = []

        for func in self.commands[command]:
            responses.append(await func(node, *req_split[1:]))

        return b" | ".join(responses)

    @check_args
    async def subscribe_node(self, node: KVNode, port: int) -> bytes:
        self.nodes_capacities.put((0, node.host))
        node.port = port
        self.nodes[node.host] = node
        self.logger.info(f"subscribed {node.host}")

        return b"OK"

    @subscribtion_required
    @check_args
    async def write_data(self, node: KVNode, key: str, value: str) -> bytes:

        node = await self.get_least_node()

        await self.file.write(f"{key}|{node.ip}\n")
        self.keys_map[key] = node.host
        await self.write_to_client(node, key, value)

        return b"OK"

    @check_args
    async def get_data(self, node: KVNode, key: str):
        await node.write(f"get {key}")
        data = await node._reader.readline()
        node.disconnect()

        return data

    async def get_least_node(self) -> KVNode:
        known_capacity = 0
        capacity = -1
        while known_capacity != capacity:
            known_capacity, node_host = self.nodes_capacities.get(block=False)

            node = self.nodes[node_host]

            capacity = await node.get_capacity()
            node.disconnect()

            self.nodes_capacities.put((capacity, node_host))

        capacity, node_host = self.nodes_capacities.get(block=False)

        return self.nodes[node_host]

    async def write_to_client(self, node: KVNode, key: str, value: str):
        await node.connect()
        await node.write_data(key, value)
        node.disconnect()

        self.logger.info(f"write {key}:{value} to {node.host}")


if __name__ == "__main__":
    server = Server("127.0.0.1", 8000)
    asyncio.run(server.start_server())
