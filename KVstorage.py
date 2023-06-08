import sys
import asyncio
import subprocess
from KVclient import to_bytes


async def check_server(host, port):
    try:
        reader, writer = await asyncio.open_connection(host, port)
        writer.close()
        await writer.wait_closed()
        return True
    except (ConnectionRefusedError, TimeoutError, OSError):
        return False


async def main():
    host, port = "localhost", 8001
    if not await check_server(host, port):
        subprocess.Popen(["python", "KVclient.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return

    reader, writer = await asyncio.open_connection(host, port)
    writer.write(to_bytes("send " + " ".join(sys.argv[1:])))
    await writer.drain()

    response = (await reader.readline()).decode()[:-1]
    print(response)
    writer.close()


if __name__ == "__main__":
    asyncio.run(main())