#!/usr/bin/env python3
import asyncio
import aioflureedb

async def blockmon(block):
    print("BLOCK DONE: ", block)

async def new_user(obj_id, flakes, new_obj, operation):
    print("NEW ROLE:", obj_id, flakes)
    print("        :", new_obj, operation)

async def dropped_user(obj_id, flakes, old_obj, operation):
    print("DROPPED ROLE:", obj_id, flakes)
    print("        :", old_obj, operation)

async def updated_user(obj_id, flakes, old_obj, new_obj, operation):
    print("UPDATED ROLE:", obj_id, flakes)
    print("        :", old_obj, new_obj, operation)

async def blocks_demo():
    print("Connecting to FlureeDB")
    async with aioflureedb.FlureeClient(port=8090) as flureeclient:
        print("Waiting till Fluree is ready")
        await flureeclient.health.ready()
        print("Looking up database")
        db = await flureeclient["dla/base"]
    print("Opening database")
    async with db() as database:
        print("Initializing monitor")
        database.monitor_init(blockmon, start_block=None,rewind=1000000)
        database.monitor_register_create("_role", new_user)
        database.monitor_register_delete("_role", dropped_user)
        database.monitor_register_update("_role", updated_user)
        print("Running Monitor")
        await database.monitor_untill_stopped()

LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(blocks_demo())
