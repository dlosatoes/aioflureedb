#!/usr/bin/env python3
import asyncio
import aioflureedb

async def tx_error_monitor(database, obj, operation, flakes, flake):
    print("========= _TX ERROR MONITOR =============")
    latest = await database.flureeql.query(select=["*"], ffrom=obj)
    print(latest)
    print("  -------- flakes -------")
    print(flake)
    print("=========================================")

async def user_monitor(database, obj, operation, flakes):
    print("============ USER MONITOR ===============")
    print(operation)
    if operation != "Delete":
        latest = await database.flureeql.query(select=["*"], ffrom=obj)
        print(latest[0])
    else:
        for flake in flakes:
            print(flake)
    print("=========================================")

async def blocks_demo(triggermap):
    async with aioflureedb.FlureeClient(port=8090) as flureeclient:
        await flureeclient.health.ready()
        db = await flureeclient["dla/dla"]
    async with db() as database:
        stats = await database.ledger_stats()
        # First make a dict from the _predicate collection.
        predicates = await database.flureeql.query(select=["id","name"], ffrom="_predicate")
        predicate = dict()
        for pred in predicates:
            predicate[pred["_id"]] = pred["name"]
        # Get the currently last block
        noblocks = True
        if "status" in stats and stats["status"] == 200 and "data" in stats and "block" in stats["data"]:
            startblock = stats["data"]["block"]
        else:
            raise RuntimeError("Invalid initial response from ledger_stats")
        # Keep monitoring for any new blocks.
        while True:
            # If we had zero blocks to process the last time around, wait a full second before polling again if there are
            #  new blocks.
            if noblocks:
                await asyncio.sleep(1)
            noblocks = True
            # Get the latest ledger stats.
            stats = await database.ledger_stats()
            if "status" in stats and stats["status"] == 200 and "data" in stats and "block" in stats["data"]:
                endblock = stats["data"]["block"]
                if endblock > startblock:
                    # Process the new blocks
                    noblocks = False
                    for block in range(startblock + 1, endblock + 1):
                        print("---------------- BLOCK", block, "--------------------")
                        grouped = dict()
                        # Fetch the new block
                        block_data = await database.block.query(block=block)
                        # Itterate all flakes.
                        for flake in block_data[0]["flakes"]:
                            predno = flake[1]
                            # Patch numeric predicates to textual ones.
                            if predno in predicate:
                                flake[1] = predicate[predno]
                            else:
                                raise RuntimeError("Need a restart after new predicates are added to the database")
                            # Group the flakes together by object.
                            if not flake[0] in grouped:
                                grouped[flake[0]] = list()
                            grouped[flake[0]].append(flake)
                        # Process per object.
                        for obj in grouped:
                            untriggered = True
                            # Ectract the collection name
                            collection = grouped[obj][0][1].split("/")[0]
                            has_true = False
                            has_false = False
                            for flake in grouped[obj]:
                                if flake[4]:
                                    has_true = True
                                else:
                                    has_false = True
                            operation = None
                            if has_true:
                                if has_false:
                                    operation = "Update"
                                else:
                                    operation = "Create/Set"
                            elif has_false:
                                operation = "Delete"
                            # Trigger on collection if in map
                            if collection in triggermap:
                                await triggermap[collection](database, obj, operation, grouped[obj])
                                untriggered = False
                            # Trigger on predicate if in map
                            for flake in grouped[obj]:
                                if untriggered and flake[1] in triggermap:
                                    await triggermap[flake[1]](database, obj, operation, grouped[obj], flake)
                                    untriggered = False
                        # Set the new start block.
                        startblock = block

MAP = {
    "_user": user_monitor,
    "_tx/error": tx_error_monitor
}
LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(blocks_demo(MAP))
