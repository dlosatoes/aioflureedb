#!/usr/bin/python3
"""Basic asynchonous client library for FlureeDB"""
import asyncio
import json
import aiohttp
from aioflureedb.signing import DbSigner
class FlureeException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)


class FlureeDatabase:
    """Basic asynchonous client for FlureeDB for non-database specific APIs"""
    def __init__(self,
                 privkey,
                 auth_address,
                 host="localhost",
                 port=8080,
                 https=False,
                 sig_validity=120,
                 sig_fuel=1000):
        """Constructor

        Parameters
        ----------
        privkey : string
                  Hex or base58 encoded signing key
        auth_address : string
                       key-id of the signing key
        host : string
                   hostname of the FlureeDB server. Defaults to localhost.
        port : int
                   port of the FlureeDB server. Defaults to 8080
        https : bool
                   Boolean indicating flureeDB is running behind a HTTPS proxy
        sig_validity : int
                   Validity in seconda of the signature.
        sig_fuel : int
                   Not sure what this is for, consult FlureeDB documentation for info.
        """
        self.host = host
        self.port = port
        self.https = https
        self.signer = DbSigner(privkey, auth_address, database, sig_validity, sig_fuel)
        self.session = aiohttp.ClientSession()
        self.known_endpoints = set(["dbs","new_db","delete_db","add_server","remove_server","health","new_keys"])
        self.implemented = set()
    def __getattr__(self, api_endpoint):
        """Select API endpoint

        Parameters
        ----------
        api_endpoint : string
                     Name of the API endpoint.

        Returns
        -------
        object
            Endpoint object suitable for API endpoint.
        """
        if api_endpoint not in self.known_endpoints:
            raise AttributeError("FlureeDB has no endpoint named " + api_endpoint)
        if api_endpoint not in self.implemented:
            raise NotImplementedError("No implementation yet for " + api_endpoint)
    async def close_session(self):
        """Close HTTP(S) session to FlureeDB"""
        await self.session.close()



# pylint: disable=too-few-public-methods
class FlureeDbClient:
    """Basic asynchonous client for FlureeDB representing a particular database on FlureeDB"""
    # pylint: disable=too-many-arguments
    def __init__(self,
                 privkey,
                 auth_address,
                 database,
                 host="localhost",
                 port=8080,
                 https=False,
                 sig_validity=120,
                 sig_fuel=1000):
        """Constructor

        Parameters
        ----------
        privkey : string
                  Hex or base58 encoded signing key
        auth_address : string
                       key-id of the signing key
        database : string
                   net/db string of the flureeDB database
        host : string
                   hostname of the FlureeDB server. Defaults to localhost.
        port : int
                   port of the FlureeDB server. Defaults to 8080
        https : bool
                   Boolean indicating flureeDB is running behind a HTTPS proxy
        sig_validity : int
                   Validity in seconda of the signature.
        sig_fuel : int
                   Not sure what this is for, consult FlureeDB documentation for info.
        """
        self.database = database
        self.host = host
        self.port = port
        self.https = https
        self.signer = DbSigner(privkey, auth_address, database, sig_validity, sig_fuel)
        self.session = aiohttp.ClientSession()
        self.known_endpoints = set(["snapshot","list_snapshots","export","query","multi_query","block","history","transact","graphql","sparql","command","reindex","hide","gen_flakes","query_with","test_transact_with","block_range_with","ledger_stats","storage","pw"])
        self.pw_endpoints = set(["generate","renew","login"])
        self.implemented = set(["query", "command"])

    async def close_session(self):
        """Close HTTP(S) session to FlureeDB"""
        await self.session.close()

    def __getattr__(self, api_endpoint):
        """Select API endpoint

        Parameters
        ----------
        api_endpoint : string
                     Name of the API endpoint.

        Returns
        -------
        object
            Endpoint object suitable for API endpoint.
        """
        class _StringEndpoint:
            def __init__(self, api_endpoint, client):
                """Constructor

                Parameters
                ----------
                api_endpoint : string
                               Name of the API endpoint
                client: object
                        The wrapping FlureeDbClient
                """
                secure = ""
                if client.https:
                    secure = "s"
                self.url = "http" + \
                           secure + \
                           "://" + \
                           client.host + \
                           ":" + \
                           str(client.port) + \
                           "/fdb/" + \
                           client.database + \
                           "/" + \
                           api_endpoint
                self.signer = client.signer
                self.session = client.session

            async def _post_body_with_headers(self, body, headers):
                """Internal, post body with HTTP headers

                Parameters
                ----------
                body : string
                       HTTP Body string
                headers : dict
                          Key value pairs to use in HTTP POST request

                Returns
                -------
                string
                    Content as returned by HTTP server
                """
                async with self.session.post(self.url, data=body, headers=headers) as resp:
                    if resp.status != 200:
                        raise FlureeException(await resp.text())
                    return await resp.text()

            async def header_signed(self, query_body):
                """Do a HTTP query using headers for signing

                Parameters
                ----------
                query_body : dict/list/string
                       query body to sign using headers.

                Returns
                -------
                string
                    Return body from server
                """
                body, headers, _ = self.signer.sign_query(query_body)
                return await self._post_body_with_headers(body, headers)

            async def body_signed(self, transact_obj):
                """Do a HTTP query using body envelope for signing
                Parameters
                ----------
                transact_obj : list
                       transaction to sign using body envelope.

                Returns
                -------
                string
                    Return body from server

                """
                command = self.signer.sign_transaction(transact_obj)
                body = json.dumps(command, indent=4, sort_keys=True)
                headers = {"content-type": "application/json"}
                return await self._post_body_with_headers(body, headers)

        # pylint: disable=too-few-public-methods
        class FlureeQlEndpoint:
            """Endpoint for JSON based (FlureeQl) queries"""
            def __init__(self, api_endpoint, client):
                """Constructor

                Parameters
                ----------
                api_endpoint : string
                               Name of the API endpoint
                client: object
                        The wrapping FlureeDbClient
                """
                self.stringendpoint = _StringEndpoint(api_endpoint, client)
            class FlureeQlQuery:
                class ObjSetter:
                    def __init__(self, query, key):
                        self.query = query
                        self.key = key
                    def __call__(self, value):
                        self.query.obj[self.key] = value
                def __init__(self, endpoint):
                    self.endpoint = endpoint
                    self.obj = dict()
                    self.permittedkeys = set(["select","selectOne","selectDistinct","where","block","prefixes","vars","opts"])
                async def __call__(self, obj):
                    return await self.endpoint.actual_query(obj)
                def __getattr__(self, fqlkey):
                    if fqlkey in self.permittedkeys:
                        return ObjSetter(this, fqlkey)
                    else:
                        raise AttributeError("FlureeQl query has no key defined named " + fqlkey)
            def __getattr__(self, method):
                if api_endpoint == "select":
                    return FlureeQlQuery(this)
                else:
                    raise AttributeError("FlureeQlEndpoint has no attribute named " + method)
            async def actual_query(self, query_object):
                """Query wit a python dict that should get JSON serialized and convert JSON
                   response back into a pthhon object

                Parameters
                ----------
                query_object : dict
                               JSON serializable query

                Returns
                -------
                dict
                    JSON decoded query response
                """
                return_body = await self.stringendpoint.header_signed(query_object)
                return json.loads(return_body)

        class TransactionEndpoint:
            """Endpoint for FlureeQL queries"""
            def __init__(self, api_endpoint, client):
                """Constructor

                Parameters
                ----------
                api_endpoint : string
                               Name of the API endpoint
                client: object
                        The wrapping FlureeDbClient
                """
                self.stringendpoint = _StringEndpoint(api_endpoint, client)

            async def transaction(self, transaction_obj):
                """Transact with list of python dicts that should get serialized to JSON,
                returns a transaction handle for polling FlureeDB if needed.

                Parameters
                ----------
                transaction_obj : list
                               Transaction list

                Returns
                -------
                string
                    transactio ID of pending transaction
                """
                return await self.stringendpoint.body_signed(transaction_obj)

            async def transaction_checked(self, transaction_obj):
                """Transact with list of python dicts that should get serialized to JSON,
                returns an awaitable that resolves only when the transaction has finaly
                failed or succeeded.

                Parameters
                ----------
                transaction_obj : list
                               Transaction list

                Returns
                -------
                bool
                    success or failure if the treansaction

                Raises
                ------
                NotImplementedError
                    Method isn't actually implemented yet.
                """
                raise NotImplementedError("No checked transactions implemented so far.")

        if api_endpoint not in self.known_endpoints:
            raise AttributeError("FlureeDB has no endpoint named " + api_endpoint) 
        if api_endpoint not in self.implemented:
            raise NotImplementedError("No implementation yet for " + api_endpoint)
        if api_endpoint in ["command"]:
            return TransactionEndpoint(api_endpoint, self)
        return FlureeQlEndpoint(api_endpoint, self)
