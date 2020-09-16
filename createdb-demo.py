#!/usr/bin/python3
# WARNING: This script isn't meant to have any end-user use. It's a walkthrough
# script for lib-dev use only.
import asyncio
import hashlib
import aioflureedb

async def fluree_demo(privkey, addr):
    flureeclient = aioflureedb.FlureeClient(privkey, addr, port=8090, dryrun=False)
    await flureeclient.health.ready()
    await flureeclient.new_db(db_id="dev/test12")
    await flureeclient.close_session()

PKEY = "1209d093f76eab468ef299d2b8dc2f7ea26c0362c4d79419ff84fd33e49e55bb"
ADDRESS = "Tf2U74MjReejggyrbSBVwpyk5waEfcq7qkL"
LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(fluree_demo(PKEY, ADDRESS))



