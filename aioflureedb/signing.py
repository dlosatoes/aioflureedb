#!/usr/bin/python3
"""Low level signing library for FlureeDB signatures"""
import json
import random
import time
import base64
import hashlib
from email.utils import formatdate
from datetime import datetime
from time import mktime
import base58
from ellipticcurve import privateKey, ecdsa


def _to_hex(x):
    return "".join([hex(ord(c))[2:].zfill(2) for c in x])


class DbSigner:
    """Low level signer class for signing FlureeDB transactions and queries"""
    # pylint: disable=too-many-arguments
    def __init__(self, privkey, address, database, validity=120, fuel=1000):
        if len(privkey) != 64:
            privkey = base58.b58decode(privkey).hex()
        self.private_key = privateKey.PrivateKey.fromString(bytes.fromhex(privkey))
        self.public_key = self.private_key.publicKey()
        self.auth_id = address
        self.database = database
        self.validity = validity
        self.fuel = fuel

    def _string_signature(self, datastring):
        sig = ecdsa.Ecdsa.sign(datastring, self.private_key, with_recid=True)
        derstring = sig.toDer()
        hexder = _to_hex(derstring)
        command = dict()
        command["cmd"] = datastring
        command["sig"] = hexder
        return command

    def _obj_signature(self, obj):
        rval = self._string_signature(json.dumps(obj))
        return rval

    def sign_transaction(self, transaction):
        """Sign a FlureeDB transaction for use in thr command endpoint"""
        obj = dict()
        obj["type"] = "tx"
        obj["tx"] = transaction
        obj["db"] = self.database
        obj["auth"] = self.auth_id
        obj["fuel"] = self.fuel
        nonce = random.randint(0, 9007199254740991)
        obj["nonce"] = nonce
        obj["expire"] = int(time.time() + self.validity)
        rval = self._obj_signature((obj))
        return rval

    def sign_query(self, param, querytype="query"):
        """Sign a FlureeDB query"""
        body = json.dumps(param)
        uri = "/fdb/" + self.database + "/" + querytype
        stamp = mktime(datetime.now().timetuple())
        mydate = formatdate(timeval=stamp, localtime=False, usegmt=True)
        hsh = hashlib.sha256()
        hsh.update(body.encode())
        digest = hsh.digest()
        b64digest = base64.b64encode(digest).decode()
        signingstring = "(request-target): post " + uri + "\nx-fluree-date: " + mydate + "\ndigest: SHA-256=" + b64digest
        sig = ecdsa.Ecdsa.sign(signingstring, self.private_key, with_recid=True)
        derstring = sig.toDer()
        hexder = _to_hex(derstring)
        headers = dict()
        headers["debug-signing-string"] = signingstring
        headers["content-type"] = "application/json"
        headers["mydate"] = mydate
        headers["signature"] = 'keyId="na",headers="(request-target) host mydate digest",algorithm="ecdsa-sha256",signature=' + hexder
        headers["digest"] = "SHA256=" + b64digest
        return body, headers, uri
