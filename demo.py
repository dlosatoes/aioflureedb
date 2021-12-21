#!/usr/bin/python3
# WARNING: This script isn't meant to have any end-user use. It's a walkthrough
# script for lib-dev use only.
import asyncio
import hashlib
import time
import subprocess
import aioflureedb
import bitcoinlib
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

def get_key_id_from_privkey(privkey):
    if privkey:
        # Try generating the key-id from the private key using starkbank-ecdsa
        private_key = privateKey.PrivateKey.fromString(bytes.fromhex(privkey))
        public_key = private_key.publicKey()
        # 512 bit version of the public key prefixed with \x04
        pk = b'\x04' + public_key.toString().encode("latin1")
        # Take the ripemd160 hash of the sha256 hash of the public key
        h3 = hashlib.sha256()
        h4 = h2 = hashlib.new('ripemd160')
        h3.update(pk)
        h4.update(h3.digest())
        # Prefix the 160 bit ripmd160 hash with the fluree network id x0f02
        core2 =  b'\x0f\x02' + h4.digest()
        #Take the sha256 of the sha256 of the prefixed ripemd160 hash
        h5 = hashlib.sha256()
        h6 = hashlib.sha256()
        h5.update(core2)
        h6.update(h5.digest())
        # Use the first 4 characters as checksum, add to the prefixed ripmd160 hash and base58 encode the result.
        keyid2 = base58.b58encode(core2 + h6.digest()[:4]).decode()

        # Doing the same with bitcoinlib
        # Remove the bitcoin network id and the checksum from the base58 decoded bitcoin adress,
        # then prefix with fluree network id. 
        core = b'\x0f\x02' + base58.b58decode(bitcoinlib.keys.Key(privkey).address())[1:-4]
        #Take the sha256 of the sha256 of the decoded and patched bitcoin adress.
        h1 = hashlib.sha256()
        h2 = hashlib.sha256()
        h1.update(core)
        h2.update(h1.digest())
        # Use the first 4 characters as checksum, base58 encode the result.
        keyid = base58.b58encode(core + h2.digest()[:4]).decode()

        # Check is they are the same
        if keyid != keyid2:
            print("OOPS", keyid, "!=", keyid2)
            print(core.hex())
            print(core2.hex())
            print()
        else:
            print("IDENTICAL")
            print()
        # Return the bitcoinlib version
        return keyid
    return None

async def fluree_demo(privkey, addr):
    print(privkey)
    print(addr)
    async with aioflureedb.FlureeClient(privkey, addr, port=8090) as flureeclient:
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
ADDRESS = get_key_id_from_privkey(PKEY)
LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(fluree_demo(PKEY, ADDRESS))



