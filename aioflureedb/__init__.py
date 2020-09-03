#!/usr/bin/python3
"""Basic asynchonous client library for FlureeDB"""
import asyncio
import json
import aiohttp
from aioflureedb.signing import DbSigner


# pylint: disable=too-few-public-methods
class FlureeDbClient:
    """Basic asynchonous client library for FlureeDB"""
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
                    assert resp.status == 200
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
        class JSONEndpoint:
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

            async def query(self, query_object):
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
                return_body = await self.stringendpoint.header_signed(json.dumps(query_object))
                return json.loads(return_body)

        class SparQlEndpoint:
            """Endpoint for SparQL queries"""
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

            async def query(self, query_sparql_obj):
                """Query wit a SparQl representation object that should get serialized and convert
                   response back into a suitable object"""
                raise NotImplementedError("No SparQL support yet in library")

        class GraphQlEndpoint:
            """Endpoint for GraphQl queries"""
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

            async def query(self, query_graphql_obj):
                """Query wit a GraphQL representation object that should get serialized and convert
                   response back into a suitable object"""
                raise NotImplementedError("No GraphQL support yet in library")

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

        print("DEBUG:", api_endpoint)
        if api_endpoint in ["graphql"]:
            return GraphQlEndpoint(api_endpoint, self)
        if api_endpoint in ["sparql"]:
            return SparQlEndpoint(api_endpoint, self)
        if api_endpoint in ["command"]:
            return TransactionEndpoint(api_endpoint, self)
        return JSONEndpoint(api_endpoint, self)
