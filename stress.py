'''
just getting a quick and dirty feel for how long it would take a
"master" aspyre node to receive 100 SHOUT messages from 100 clients
'''

import logging
import asyncio
import pyre

async def client():
    # this will automatically start the pyre engine
    try:
        async with pyre.Pyre() as node:
            await node.join("stress")
            try:
                await asyncio.sleep(3)
                await node.shout("stress", b"test")
                await asyncio.sleep(3)
            finally:
                await node.leave("stress")
    finally:
        pass

async def spawn_clients():
    try:
        tasks = []
        for x in range(2):
            tasks.append(client())

        await asyncio.gather(*tasks)
    finally:
        pass

shout_messages_received = 0
async def receiver(node, message):
    print(message)
    if message[0] == b"SHOUT":
        globals()["shout_messages_received"] += 1
        print(f"SHOUT Messages Received :: [{globals()['shout_messages_received']}]")
        if globals()["shout_messages_received"] == 1:
            node.stop_listening()

async def main():
    # this will automatically start the pyre engine
    try:
        async with pyre.Pyre() as node:               
            await node.join("stress")
            try:
                tasks = [
                    spawn_clients(),
                    node.listen(receiver)
                ]

                await asyncio.gather(*tasks)
            finally:
                await node.leave("stress")
    finally:
        pass

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s'
    # Create a StreamHandler for debugging
    logger = logging.getLogger("pyre")
    logger.setLevel(logging.DEBUG)
    # i.e. logging.DEBUG, logging.WARNING
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)
    logger.propagate = False
    
    logger = logging.getLogger("pyre_node")
    logger.setLevel(logging.DEBUG)
    # i.e. logging.DEBUG, logging.WARNING
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)
    logger.propagate = False

    logger = logging.getLogger("zbeacon")
    logger.setLevel(logging.DEBUG)
    # i.e. logging.DEBUG, logging.WARNING
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)
    logger.propagate = False

    asyncio.run(main())