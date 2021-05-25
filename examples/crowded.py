'''
lots of clients ( crowded )

start the server ( where SHOUT message originates )
shut down once all clients receive the SHOUT
'''

import logging
import uuid
import asyncio
import aspyre

GROUP = "crowded"
MESSAGE = b"just some data"

async def client_receiver(node, message):
    if message[0] == b"SHOUT":
        node.stop_listening()

async def client():
    # this will automatically start the pyre engine
    async with aspyre.Pyre() as node:
        await node.join(GROUP)
        try:
            # need some time to allow for convergence
            await asyncio.sleep(1.0)

            # run aspyre for 10 seconds
            await asyncio.wait_for(node.listen(client_receiver), timeout=10)
        finally:
            # await work_task
            await node.leave(GROUP)

async def server():
    # this will automatically start the pyre engine
    async with aspyre.Pyre() as node:
        await node.join(GROUP)        
        try:
            # need some time to allow for convergence
            await asyncio.sleep(1.0)
            await node.shout(GROUP, MESSAGE)
            # can't leave too quickly after sending the shout
            # or the peers will drop the connection prior
            # to receiving the message
            await asyncio.sleep(1.0)
        finally:
            # await work_task
            await node.leave(GROUP)

async def main():
    tasks = [        
        server()
    ]

    [tasks.append(client()) for x in range(7)]

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