import logging
import asyncio
import pyre

'''
the pyre engine is always receiving
this is the callback that will be used by that task
to display messages
'''
async def receiver(messages):
    print(messages)

'''
work task
just exercise different parts of the API
'''
async def work(pyre):
    print("start work")
    await asyncio.sleep(1)
    await pyre.join("blah")
    await asyncio.sleep(1)
    await pyre.shout("blah", b"look at this shout message")
    await asyncio.sleep(1)
    peers = pyre.get_peers()
    peer = list(peers)[0]
    print(pyre.peer_address(peer))
    await pyre.whisper(list(peers)[0], b"look at this whisper message")
    await asyncio.sleep(1)
    await pyre.leave("blah")
    print("done work")

'''
this example uses two tasks
one task to run the pyre engine
the other task to do some work
'''
async def main():
    async with pyre.Pyre(receiver) as node:
        # start pyre node
        # this will start the pyre engine
        # send/receive beacons
        # receive messages
        # etc
        pyre_node_work = asyncio.create_task(node.run())
        
        # start work
        # going to wait for it to finish
        await asyncio.create_task(work(node))

if __name__ == '__main__':
    # Create a StreamHandler for debugging
    logger = logging.getLogger("pyre")
    logger.setLevel(logging.DEBUG)
    # i.e. logging.DEBUG, logging.WARNING
    logger.addHandler(logging.StreamHandler())
    logger.propagate = False

    asyncio.run(main())