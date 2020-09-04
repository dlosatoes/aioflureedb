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

async def main(clnt):
    print("MAIN")
    randomuser = "user-" + str(int(time.time()) % 10000)
    print("CREATING RANDOM USER:", randomuser)
    try:
        transaction = await client.command.transaction([{"_id":"_user","username": randomuser}])
        # Proposed syntactic sugar #1
        # transaction = await client.command.transaction.create("_user")(username=randomuser).execute()
        print("OK: TRANSACTION STARTED:", transaction)
    except Exception as exp:
        print("FAIL: TRANSACTION FAILED:", exp)
    print()
    print("LOOKING UP ALL USERS")
    try:
        result = await client.query.query({"select": ["*"],"from": "_user"})
        # Change to allow for syntactic sugar #2 and #3
        #result = await client.query.query.obj({"select": ["*"],"from": "_user"})
        #
        # Proposed syntactic sugar #2
        #result = await client.query.query(select=["*"], from="_user")
        # 
        # Proposed syntactic sugar #3
        #result = await client.query.query.select(["*"]).from("_user").execute()
        #
        print("OK: QUERY SUCCEDED:", result)
    except Exception as exp:
        print("FAIL: QUERY FAILED:", exp)
    print()
    await clnt.close_session()

pkey = get_privkey_from_running_docker()
print("Getting address from key")
address = get_key_id_from_privkey(pkey)
print("ADDRESS:", address, pkey)
print()
database = "dla/dla"
port = 8090
client = aioflureedb.FlureeDbClient(pkey, address, database, port=8090, dryrun=False)
loop = asyncio.get_event_loop()
loop.run_until_complete(main(client))

