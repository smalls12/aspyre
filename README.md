[![Pylint](https://github.com/smalls12/aspyre/actions/workflows/pylint.yml/badge.svg)](https://github.com/smalls12/aspyre/actions/workflows/pylint.yml)

# aspyre
Asyncio Pyre

This is just a work in progress.
I am taking the current pyre code base and making it run with asyncio.
https://github.com/zeromq/pyre

Maintaining all copyrights as I am reusing alot of code.

Check the wiki.

## goals

1. Reimplement pyre so that internally it uses asyncio [ DONE ]
2. Implement https://rfc.zeromq.org/spec/43/ [ WIP ]<br>
    a. Implement CURVE Encryption [ DONE ]<br>
    b. Implement IPv6 Beacons<br>
3. Implement zyre ( https://github.com/zeromq/zyre ) elections

## status

example code has been checked in showing how some simple aspyre nodes can be
created using no encryption ( default ) or with encryption.

i'm currently just working on cleaning up the code.

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
import aspyre

async def receiver(node, message):
    print(message)

async def main():
    # this will automatically start the aspyre engine
    async with aspyre.Aspyre() as node:               
        await node.join("CHAT")
        try:
            try:
                # run aspyre for 10 seconds
                await asyncio.wait_for(node.listen(receiver), timeout=10)
            except asyncio.TimeoutError:
                pass
        finally:
            await node.leave("CHAT")

if __name__ == '__main__':
    asyncio.run(main())
```

## encrypted example

```python
import asyncio
import aspyre

async def receiver(node, message):
    print(message)

async def main():
    authentication = {
        "public_keys_dir": "~/path_to_public_keys",
        "server_secret_file": "~/path_to_server_secret_file",
        "client_secret_file": "~/path_to_client_secret_file"
    }

    # this will automatically start the aspyre engine
    async with aspyre.AspyreEncrypted(authentication) as node:               
        await node.join("CHAT")
        try:
            try:
                # run aspyre for 10 seconds
                await asyncio.wait_for(node.listen(receiver), timeout=10)
            except asyncio.TimeoutError:
                pass
        finally:
            await node.leave("CHAT")

if __name__ == '__main__':
    asyncio.run(main())
```