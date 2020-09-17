#!/usr/bin/python3
# WARNING: This script isn't meant to have any end-user use. It's a walkthrough
# script for lib-dev use only.
import time
import asyncio
from aioflureedb import FlureeClient

async def fluree_demo(privkey, addr):
    semi_unique_num_string = str(int(time.time()) % 10000)
    semi_unique_db = "mynet/db" + semi_unique_num_string
    user = "Bob"
    print("Creating database", semi_unique_db, "with a user", user)
    async with  FlureeClient(privkey, addr, port=8090) as flureeclient:
        await flureeclient.health.ready()
        await flureeclient.new_db(db_id=semi_unique_db)
        db = await flureeclient[semi_unique_db]
    print("database created")
    async with db(privkey, addr) as database:
        transaction = await database.command.transaction([{"_id":"_user","username": user}])
        print("User added"),
        result = await database.flureeql.query(
            select=["*"],
            ffrom="_user"
        )
        print("Users:")
        for user in result:
            print(" -", user)


PKEY = "1209d093f76eab468ef299d2b8dc2f7ea26c0362c4d79419ff84fd33e49e55bb"
ADDRESS = "Tf2U74MjReejggyrbSBVwpyk5waEfcq7qkL"
LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(fluree_demo(PKEY, ADDRESS))



