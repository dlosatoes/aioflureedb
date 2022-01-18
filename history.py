#!/usr/bin/env python3
import asyncio
import aioflureedb

async def history_demo():
    async with aioflureedb.FlureeClient(port=8090) as flureeclient:
        await flureeclient.health.ready()
        db = await flureeclient["dla/base"]
    async with db() as database:
        result = await database.reindex.query()
        print(result)
        result = await database.history.query(history=["_role/id","oidc_user"], showAuth=True)
        print(result)

LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(history_demo())
