#!/usr/bin/env python3
import asyncio
import aioflureedb

async def blockmon(block):
    print("BLOCK DONE: ", block)

async def new_user(obj, flakes):
    print("NEW USER:", obj, flakes)

async def dropped_user(obj, flakes):
    print("DROPPED USER:", obj, flakes)

async def updated_user(obj, flakes):
    print("UPDATED USER:", obj, flakes)

async def blocks_demo():
    print("Connecting to FlureeDB")
    async with aioflureedb.FlureeClient(port=8090) as flureeclient:
        print("Waiting till Fluree is ready")
        await flureeclient.health.ready()
        print("Looking up database")
        db = await flureeclient["test1/test1"]
    print("Opening database")
    async with db() as database:
        print("Initializing monitor")
        database.monitor_init(blockmon, 1, use_flakes=True)
        database.monitor_register_create("_auth", new_user)
        database.monitor_register_delete("_auth", dropped_user)
        database.monitor_register_update("_auth", updated_user)
        print("Running Monitor")
        await database.monitor_untill_stopped()

LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(blocks_demo())
