#!/usr/bin/python3
import sys
import asyncio
import aioflureedb

async def sql_demo():
    print("Connecting to FlureeDB")
    async with aioflureedb.FlureeClient(port=8090) as flureeclient:
        print("Waiting till Fluree is ready")
        await flureeclient.health.ready()
        state = await flureeclient.nw_state()
        print(state)
        version = await flureeclient.version()
        print(version)

LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(sql_demo())
