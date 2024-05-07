#!/usr/bin/python3
# WARNING: This script isn't meant to have any end-user use. It's a walkthrough
# script for lib-dev use only.
import asyncio
import time
import subprocess
import aioflureedb
from ellipticcurve import privateKey, ecdsa
import base58
import json

def get_privkey_from_running_docker():
    print("Finding fluree docker")
    dps = subprocess.check_output(['docker', 'ps']).decode().split("\n")
    did = None
    for line in dps:
        if "flureedb-single" in line:
            did = line.split(" ")[0]
    pkey = None
    if did:
        print("Getting signing key from docker instance", did)
        pkey =  subprocess.check_output(['docker', 'exec', '-it', did, '/bin/cat', 'default-private-key.txt']).decode()
        if pkey:
            print("EXTRACTED")
        else:
            print("FAIL")
    else:
        print("No running fluree docker instance found")
    print()
    return pkey

async def fluree_demo(privkey):
    print(privkey)
    async with aioflureedb.FlureeClient(privkey, port=8090) as flureeclient:
        print("AWAIT Ready")
        await flureeclient.health.ready()
        print("READY")
        print(await flureeclient.dbs())
        print("ENDPOINTS:", dir(flureeclient))
        print("HEALTH:", await flureeclient.health())
        print("########################################")
        print("NEWKEYS", await flureeclient.new_keys())
        print("########################################")
        new_db = await flureeclient.new_db(db_id="dev/test50")
        print("NEWDB", new_db)
        # Not working yet, need to look into del_db
        #   del_db = await flureeclient.delete_db(db_id="dev/test14")
        #   print("DELDB:", del_db)
        print("########################################")
        async for network in flureeclient:
            print("### NET:",network)
            for db in network:
                print("   -", db)
        db = await flureeclient["dev/test50"]
        async with db() as database:
            await asyncio.sleep(10)
            res = await database.flureeql.query(select=["name"], ffrom="_predicate")
            print("RES1:", json.dumps(res))
            print("Creating multiquery")
            mq = database.multi_query()
            print(type(mq))
            print("Expanding multi-query")
            mq.predicates(select=["name"], ffrom="_predicate")
            print("Expanding multi-query")
            mq.collections(select=["name"], ffrom="_collection")
            print("Calling multi-query")
            result = await mq.query()
            print(result)




PKEY = get_privkey_from_running_docker()
LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(fluree_demo(PKEY))



