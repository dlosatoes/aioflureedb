#!/usr/bin/python3
# WARNING: This script isn't meant to have any end-user use. It's a walkthrough
# script for lib-dev use only.
import time
import asyncio
from aioflureedb import FlureeClient, FlureeHttpError

async def fluree_demo(privkey):
    semi_unique_num_string = str(int(time.time()) % 10000)
    semi_unique_db = "mynet/db" + semi_unique_num_string
    user = "Bob"
    print("Creating database", semi_unique_db, "with a user", user)
    async with  FlureeClient(privkey, port=8090) as flureeclient:
        print("Waiting for fluree server ready")
        await flureeclient.health.ready()
        print("New DB")
        await flureeclient.new_db(db_id=semi_unique_db)
        print("Getting new DB")
        db = await flureeclient[semi_unique_db]
    print("database created")
    async with db(privkey) as database:
        print("Waiting for database ready")
        await database.ready()
        print("Adding user")
        transaction = await database.command.transaction([{"_id":"_user","username": user}])
        print("User added")
        result = await database.flureeql.query(
            select=["*"],
            ffrom="_user"
        )
        print("Users:")
        for user in result:
            print(" -", user)


PKEY = "55e386156770ceb0f0f72a529585de9fb701ff6fdc03e2bdac19d8c3aea90b5c"
LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(fluree_demo(PKEY))



