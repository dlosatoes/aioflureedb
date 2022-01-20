#!/usr/bin/python3
import sys
import asyncio
import aioflureedb

async def domain_api_demo():
    print("Connecting to FlureeDB")
    async with aioflureedb.FlureeClient(port=8090) as flureeclient:
        print("Waiting till Fluree is ready")
        await flureeclient.health.ready()
        print("Looking up database")
        db = await flureeclient["dla/base"]
    print("Opening database")
    async with db() as database:
        print("Instantiating domain API")
        domain_api = aioflureedb.FlureeDomainAPI("./api_maps.json", database)
        role = domain_api.get_api_by_role("demo_role")
        print("Testing demo query")
        result = await role.roles_and_predicates()
        print(result)

LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(domain_api_demo())
