#!/usr/bin/python3
import sys
import asyncio
import aioflureedb

async def domain_api_demo(key):
    print("Connecting to FlureeDB")
    async with aioflureedb.FlureeClient(port=8090) as flureeclient:
        print("Waiting till Fluree is ready")
        await flureeclient.health.ready()
        print("Looking up database")
        db = await flureeclient["dla/base"]
    print("Opening database")
    async with db(key) as database:
        print("Instantiating domain API")
        domain_api = aioflureedb.FlureeDomainAPI("./api_maps", database)
        print("Selecting role")
        role = domain_api.get_api_by_role("demo_role")
        print("Create transaction")
        trans1 = role.create_demo_user_role()
        print("Execute", type(trans1))
        await trans1()
        print("Create transaction")
        trans2 = role.create_demo_user(
                full_name="John Doe IV",
                email="j.f.doe@gmail.com",
                pubkey="TfB5z166pcmReVA3sfEqisjgv7pX2gefff4")
        print("Execute")
        await trans2()
        print("Create transaction")
        trans3 = role.create_demo_user(
                full_name="Jane Doe IV",
                email="j.e.m.doe@gmail.com",
                pubkey="TfB5z166pcmReVA3sfEqisjgv7pX2ge0004")
        print("Execute")
        await trans3()
        print("Run query")
        response = await role.get_demo_users()
        print(response)

if len(sys.argv) >= 2:
    key = sys.argv[1]
    LOOP = asyncio.get_event_loop()
    LOOP.run_until_complete(domain_api_demo(key))
else:
    print("Provide key")
