# aspyre
Asyncio Pyre

This is just a work in progress.
I am taking the current pyre code base and making it run with asyncio.

Maintaining all copyrights as I am reusing alot of code.

Check the wiki.

## example

```python
import asyncio
import pyre

async def work(pyre):
    print("start work")
    await asyncio.sleep(1)
    await pyre.shout("blah", b"look at this shout message")
    await asyncio.sleep(1)
    peers = pyre.get_peers()
    peer = list(peers)[0]
    print(pyre.peer_address(peer))
    await pyre.whisper(list(peers)[0], b"look at this whisper message")
    print("done work")

async def main():
    # this will automatically start the pyre engine
    async with pyre.Pyre() as node:               
        await node.join("blah")
        try:
            work_task = asyncio.create_task(work(node))
            while True:
                print(await node.recv())
        finally:
            await work_task
            await node.leave("blah")

if __name__ == '__main__':
    asyncio.run(main())
```