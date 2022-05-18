#!/usr/bin/python3
"""Low level signing library for FlureeDB signatures"""
import json
from enum import Enum
import random
import time
from time import mktime
import base64
import hashlib
import unicodedata
from email.utils import formatdate
from datetime import datetime
import base58
from ellipticcurve import privateKey, ecdsa


def _to_hex(x):
    """Convert binary string x to a hex string

    Parameters
    ----------
    x : string
        binary string

    Returns
    -------
    string
        Hex representation of the binary string.
    """
    if isinstance(x, bytes):
        return x.hex()
    return "".join([hex(ord(c))[2:].zfill(2) for c in x])


class BlockChain(Enum):
    """Enum for different bitcoin like chains"""
    BITCOIN = 1
    FLUREEDB = 2
    DASH = 3
    DOGECOIN = 4
    LITECOIN = 5
    NAMECOIN = 6
    ZCASH = 7


_net_id_map = {
  BlockChain.BITCOIN: b"\x00",
  BlockChain.FLUREEDB: b"\x0f\x02",
  BlockChain.DASH: b"\x4c",
  BlockChain.DOGECOIN: b"\x1e",
  BlockChain.LITECOIN: b"\x30",
  BlockChain.NAMECOIN: b"\x34",
  BlockChain.ZCASH: b"\x1c\xb8"
}


def pubkey_to_address(pubkey, chain):
    """Get FlureeDB address from pubkey

    NOTE: This functionality should probably (mostly) be in the ellipticcurve library.

    Parameters
    ----------
    pubkey : ellipticcurve.publicKey.PublicKey
             ECDSA pubkey
    chain : BlockChain
             The blockchain we are calculating the address for

    Returns
    -------
    string
        Base58 encoded FlureeDB address
    """
    x = pubkey.point.x.to_bytes(32, byteorder='big')
    yred = (2 + pubkey.point.y % 2).to_bytes(1, byteorder='big')
    key = yred + x
    hash1 = hashlib.sha256()
    hash2 = hashlib.new('ripemd160')
    hash1.update(key)
    hash2.update(hash1.digest())
    core = _net_id_map[chain] + hash2.digest()
    hash3 = hashlib.sha256()
    hash4 = hashlib.sha256()
    hash3.update(core)
    hash4.update(hash3.digest())
    return base58.b58encode(core + hash4.digest()[:4]).decode()


class DbSigner:
    """Low level signer class for signing FlureeDB transactions and queries"""
    # pylint: disable=too-many-arguments
    def __init__(self, privkey, database, validity=120, fuel=1000):
        """Constructor for DbSigner

        Parameters
        ----------
        privkey : string
                  Hex or base58 encoded signing key.
        database : string
                   Network/Db string for the database to use.
        validity: int
                  Time (seconds) the signature is to remain valid.
        fuel: int
              Not sure what this is for, consult FlureeDB documentation for info.
        """
        assert isinstance(validity, (float, int))
        if len(privkey) != 64:
            privkey = base58.b58decode(privkey).hex()
        # Old line from 1.0.3
        # self.private_key = privateKey.PrivateKey.fromString(bytes.fromhex(privkey))
        self.private_key = privateKey.PrivateKey.fromString(privkey)
        self.public_key = self.private_key.publicKey()
        self.auth_id = pubkey_to_address(self.public_key, BlockChain.FLUREEDB)
        self.database = database
        self.validity = validity
        self.fuel = fuel

    def _string_signature(self, datastring):
        """Internal method for signing a command string

        Parameters
        ----------
        datastring: string
                    Fluree command encoded string to create a signature for.

        Returns
        -------
        dict
            Python dict with command and signature fields.
        """
        sig = ecdsa.Ecdsa.sign(datastring, self.private_key)
        derstring = sig.toDer(withRecoveryId=True)
        hexder = _to_hex(derstring)
        command = {}
        command["cmd"] = datastring
        command["sig"] = hexder
        return command

    def _obj_signature(self, obj):
        """Internal method for signing a command object

        Parameters
        ----------
        obj: dict
                    Fluree command object to create a signature for.

        Returns
        -------
        dict
            Python dict with command and signature fields.
        """
        rval = self._string_signature(unicodedata.normalize("NFKC", json.dumps(obj)))
        return rval

    def sign_transaction(self, transaction, deps=None):
        """Sign a FlureeDB transaction for use in the command endpoint

        Parameters
        ----------
        transaction: list
                     Transaction list with objects for a FlureeDB transaction.
        deps: list
                     See flur.ee documentation.

        Returns
        -------
        dict
            Python dict with command and signature fields.
        """
        obj = {}
        obj["type"] = "tx"
        obj["tx"] = transaction
        obj["db"] = self.database
        obj["auth"] = self.auth_id
        obj["fuel"] = self.fuel
        nonce = random.randint(0, 9007199254740991)
        obj["nonce"] = nonce
        obj["expire"] = int((time.time() + self.validity)*1000)
        if deps:
            obj["deps"] = deps
        rval = self._obj_signature((obj))
        return rval

    def sign_query(self, param, querytype="query"):
        """Sign a FlureeDB query

        Parameters
        ----------
        param : dict
                Unsigned Fluree query object.
        querytype : string
                    API endpoint identifier.

        Returns
        -------
        string
            Body fot the HTTP post to fluree
        dict
            Dictionary with HTTP header fields for the HTTP post to FlureeDB
        string
            The URI used for signing.
        """
        # pylint: disable=too-many-locals
        if querytype in ["sql", "sparql"]:
            unicodedata.normalize("NFKC", param)
        else:
            body = unicodedata.normalize("NFKC", json.dumps(param, separators=(',', ':')))
        uri = "/fdb/" + "-".join(querytype.split("_"))
        if self.database:
            uri = "/fdb/" + self.database + "/" + "-".join(querytype.split("_"))
        stamp = mktime(datetime.now().timetuple())
        mydate = formatdate(timeval=stamp, localtime=False, usegmt=True)
        hsh = hashlib.sha256()
        hsh.update(body.encode())
        digest = hsh.digest()
        b64digest = base64.b64encode(digest).decode()
        signingstring = "(request-target): post " + uri + "\nx-fluree-date: " + mydate + "\ndigest: SHA-256=" + b64digest
        sig = ecdsa.Ecdsa.sign(signingstring, self.private_key)
        derstring = sig.toDer(withRecoveryId=True)
        hexder = _to_hex(derstring)
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["X-Fluree-Date"] = mydate
        headers["Signature"] = \
            'keyId="na",headers="(request-target) x-fluree-date digest",algorithm="ecdsa-sha256",signature="' + \
            hexder + \
            '"'
        headers["Digest"] = "SHA-256=" + b64digest
        return body, headers, uri
