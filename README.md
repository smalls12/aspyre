# aspyre
Asyncio Pyre

This is just a work in progress.
I am taking the current pyre code base and making it run with asyncio.
https://github.com/zeromq/pyre

Maintaining all copyrights as I am reusing alot of code.

Check the wiki.

## goals

1. Reimplement pyre so that internally it uses asyncio
2. Implement https://rfc.zeromq.org/spec/43/

## rationale

this new design for pyre is a replacement for the use of zactor, threads and pipes.

there is nothing wrong with those mechanisms, but I believe that asyncio can be
used instead and provide comparable performance while being significantly easier
to read and understand, maintain the codebase, remove custom messaging protocols and
scale better.

## dependency on asyncio

asyncio is only available in python3.6 and above, as such this project would not
be supporting older python versions.

if you are running an older version of python, you can simply continue to use pyre.

the changes are so significant that it is simply untennable to try and support both
of these mechanisms as part of a single code base.

## example

```python
import asyncio
import pyre

async def work(pyre):
    await asyncio.sleep(1)
    await pyre.shout("blah", b"look at this shout message")
    await asyncio.sleep(1)
    peers = pyre.get_peers()
    peer = list(peers)[0]
    print(pyre.peer_address(peer))
    await pyre.whisper(list(peers)[0], b"look at this whisper message")

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