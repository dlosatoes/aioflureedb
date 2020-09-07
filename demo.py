#!/usr/bin/python3
import asyncio
import hashlib
import time
import subprocess
import aioflureedb
import bitcoinlib
import base58

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

def get_key_id_from_privkey(privkey):
    if privkey:
        core = b'\x0f\x02' + base58.b58decode(bitcoinlib.keys.Key(privkey).address())[1:-4]
        h1 = hashlib.sha256()
        h2 = hashlib.sha256()
        h1.update(core)
        h2.update(h1.digest())
        keyid = base58.b58encode(core + h2.digest()[:4]).decode()
        return keyid
    return None

async def fluree_demo(privkey, addr):
    port = 8090
    flureeclient = aioflureedb.FlureeClient(privkey, addr, port=8090, dryrun=False)
    print("Client created")
    network = await flureeclient["dla"]
    print("Network OK")
    db = network["dla"]
    print("DB OK")
    database = db(privkey, addr)
    print("Database client created")
    randomuser = "user-" + str(int(time.time()) % 10000)
    print("Creating user:", randomuser)
    transaction = await database.command.transaction([{"_id":"_user","username": randomuser}])
    print("OK: Transaction completed,", transaction)
    result = await database.query.query(
        select=["*"],
        ffrom="_user"
    )
    print("Query succeeded, user count =", len(result))
    await flureeclient.close_session()
    await database.close_session()



PKEY = get_privkey_from_running_docker()
ADDRESS = get_key_id_from_privkey(PKEY)
LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(fluree_demo(PKEY, ADDRESS))



