#!/usr/bin/python3
import asyncio
import time
import subprocess
import aioflureedb
import bitcoinlib

def get_privkey_from_running_docker():
    dps = subprocess.check_output(['docker', 'ps']).decode().split("\n")
    did = None
    for line in dps:
        if "flureedb-single" in line:
            did = line.split(" ")[0]
    pkey = None
    if did:
        print(did)
        pkey =  subprocess.check_output(['docker', 'exec', '-it', did, '/bin/cat', 'default-private-key.txt']).decode()
    return pkey

def get_key_id_from_privkey(privkey):
    if privkey:
        return bitcoinlib.keys.Key(privkey).address()
    return None

async def main(clnt):
    print("MAIN")
    randomuser = "user-" + str(int(time.time()) % 10000)
    print("CREATING RANDOM USER:", randomuser)
    transaction = await client.command.transaction([{"_id":"_user","username": randomuser}])
    print("TRANSACTION:", transaction)
    result = await client.query.query({"select": ["*"],"from": "_user"})
    print("RESULT:", result)

pkey = get_privkey_from_running_docker()
address = get_key_id_from_privkey(pkey)
database = "dla/dla"
port = 8090
client = aioflureedb.FlureeDbClient(pkey, address, database, port=8090)
loop = asyncio.get_event_loop()
loop.run_until_complete(main(client))

