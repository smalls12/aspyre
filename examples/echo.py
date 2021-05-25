'''
simple echo app
2 aspyre nodes are started
the server shouts a message
the client will whisper it back
'''

import logging
import uuid
import asyncio
import aspyre

GROUP = "echo"
MESSAGE = b"whisper this back to me"

async def client_receiver(node, message):
    if message[0] == b"SHOUT":
        await node.whisper(uuid.UUID(bytes=message[1]), message[4])
        await asyncio.sleep(1.0)
        node.stop_listening()

async def client():
    # this will automatically start the pyre engine
    async with aspyre.Pyre() as node:
        await node.join(GROUP)
        await asyncio.sleep(1.0)
        try:
            # run aspyre for 10 seconds
            await asyncio.wait_for(node.listen(client_receiver), timeout=10)
        finally:
            # await work_task
            await node.leave(GROUP)

async def server_receiver(node, message):
    if message[0] == b"WHISPER":
        if message[3] == MESSAGE:
            await asyncio.sleep(1.0)
            node.stop_listening()

async def server():
    # this will automatically start the pyre engine
    async with aspyre.Pyre() as node:
        await node.join(GROUP)
        await asyncio.sleep(1.0)
        await node.shout(GROUP, MESSAGE)
        try:
            # run aspyre for 10 seconds
            await asyncio.wait_for(node.listen(server_receiver), timeout=10)
        finally:
            # await work_task
            await node.leave(GROUP)

async def main():
    tasks = [
        client(),
        server()
    ]

    await asyncio.gather(*tasks)

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s'
    # Create a StreamHandler for debugging
    logger = logging.getLogger("aspyre")
    logger.setLevel(logging.DEBUG)
    # i.e. logging.DEBUG, logging.WARNING
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)
    logger.propagate = False

    asyncio.run(main())