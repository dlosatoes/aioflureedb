#!/usr/bin/python3
import json
import random
import time
import base58
import base64
import hashlib
from email.utils import formatdate
from datetime import datetime
from time import mktime
from ellipticcurve import privateKey, ecdsa

class DbSigner:
    def __init__(self, privkey, address, db, validity=120, fuel=1000):
        if len(privkey) != 64:
            privkey = base58.b58decode(privkey).hex()
        self.private_key = privateKey.PrivateKey.fromString(bytes.fromhex(privkey))
        self.public_key = self.private_key.publicKey()
        self.auth_id = address
        self.db = db
        self.validity = validity
        self.fuel = fuel
    def string_signature(self, datastring):
        sig = ecdsa.Ecdsa.sign(datastring, self.private_key, with_recid=True)
        derstring = sig.toDer()
        toHex = lambda x:"".join([hex(ord(c))[2:].zfill(2) for c in x])
        hexder = toHex(derstring)
        command = dict()
        command["cmd"] = datastring
        command["sig"] = hexder
        return command
    def obj_signature(self, obj):
        rval =  self.string_signature(json.dumps(obj))
        return rval
    def sign_transaction(self, transaction):
        obj = dict()
        obj["type"] = "tx"
        obj["tx"] = transaction
        obj["db"] = self.db
        obj["auth"] = self.auth_id
        obj["fuel"] = self.fuel
        nonce = random.randint(0,9007199254740991)
        obj["nonce"] = nonce
        obj["expire"] = int(time.time() + self.validity)
        rval = self.obj_signature((obj))
        return rval
    def sign_query(self, param, querytype="query"):
        body = json.dumps(param) 
        uri = "/fdb/" + self.db + "/" + querytype;
        now = datetime.now()
        stamp = mktime(now.timetuple())
        mydate = formatdate(timeval=stamp, localtime=False, usegmt=True)
        h = hashlib.sha256()
        h.update(body.encode())
        digest = h.digest()
        b64digest = base64.b64encode(digest).decode()
        signingstring = "(request-target): post " + uri + "\nx-fluree-date: " + mydate + "\ndigest: SHA-256=" + b64digest;
        sig =  ecdsa.Ecdsa.sign(signingstring, self.private_key, with_recid=True)
        derstring = sig.toDer()
        toHex = lambda x:"".join([hex(ord(c))[2:].zfill(2) for c in x])
        hexder = toHex(derstring)
        headers = dict()
        headers["debug-signing-string"] = signingstring
        headers["content-type"] = "application/json"
        headers["mydate"] = mydate
        headers["signature"] = 'keyId="na",headers="(request-target) host mydate digest",algorithm="ecdsa-sha256",signature=' + hexder
        headers["digest"] = "SHA256=" + b64digest
        return body, headers, uri



