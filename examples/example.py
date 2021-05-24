'''
this is a long running example that also performs some work

the script will only end with a keyboard interrupt
'''

import logging
import asyncio
import pyre

async def work(node):
    await asyncio.sleep(1)
    await node.shout("blah", b"look at this shout message")
    await asyncio.sleep(1)
    try:
        peers = node.get_peers()
        peer = list(peers)[0]
        print(node.peer_address(peer))
        await node.whisper(list(peers)[0], b"look at this whisper message")
    except IndexError as e:
        pass

async def receiver(node, message):
    print(message)

async def main():
    # this will automatically start the pyre engine
    async with pyre.Pyre() as node:               
        await node.join("blah")
        try:
            tasks = [
                work(node),
                node.listen(receiver)
            ]

            await asyncio.gather(*tasks)

        finally:
            # await work_task
            await node.leave("blah")

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