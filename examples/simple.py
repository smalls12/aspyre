'''
this is just an example showing how to listen for messages for a specific
period of time

the app closes after 10 seconds
'''

import logging
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