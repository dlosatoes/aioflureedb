#!/usr/bin/python3
"""Basic asynchonous client library for FlureeDB"""
import asyncio
import json
import aiohttp
from aioflureedb.signing import DbSigner
class FlureeException(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)

class _FlureeQlQuery:
    def __init__(self, endpoint, method):
        self.endpoint = endpoint
        self.permittedkeys = set(["select","selectOne","selectDistinct","from", "where","block","prefixes","vars","opts"])
    async def __call__(self, **kwargs):
        obj = dict()
        for key, value in kwargs.items():
            if key == "ffrom":
                key = "from"
            if key not in self.permittedkeys:
                raise TypeError("FlureeQuery got unexpected keyword argument '" + key + "'")
            obj[key] = value
        return await self.endpoint.actual_query(obj)
    async def raw(self, obj):
        return await self.endpoint.actual_query(obj)

class _UnsignedGetter:
    def __init__(self, session, url):
        self.session = session
        self.url = url
    async def __call__(self):
        if self.session:
            async with self.session.get(self.url) as resp:
                if resp.status != 200:
                    raise FlureeException(await resp.text())
                response = await resp.text()
                return json.loads(response)
        else:
            print("############# GET ##############")
            print("url:", self.url)
            print("################################")
            return({"dryrun": true})

class _SignedPoster:
    def __init__(self, session, signer, url, required, optional, unsigned):
        self.session = session
        self.signer = signer
        self.url = url
        self.required = required
        self.optional = optional
        self.unsigned = unsigned
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
            if self.session:
                async with self.session.post(self.url, data=body, headers=headers) as resp:
                    if resp.status != 200:
                        raise FlureeException(await resp.text())
                    data = await resp.text()
                    try:
                        return json.loads(data)
                    except:
                        return data
            else:
                print("url:", self.url)
                print("########### HEADERS ############")
                for key in headers:
                    print(key,":",headers[key])
                print("############ BODY ##############")
                print(body)
                print("################################")
                return('{"dryrun": true}')
    async def __call__(self, **kwargs):
        kwset = set()
        kwdict = dict()
        for key, value in kwargs.items():
            if not (key in self.required or key in self.optional):
                raise TypeError("SignedPoster got unexpected keyword argument '" + key + "'")
            kwset.add(key)
            if key == "db_id":
                kwdict[key] = "db/id"
            else:
                kwdict[key] = val
        for reqkey in self.required:
            if not reqkey in kwset:
                raise TypeError("SignedPoster is missing one required named argument '", reqkey,"'")
        body = json.dumps(kwdict, indent=4, sort_keys=True)
        headers = {"Content-Type": "application/json"}
        if not self.unsigned:
            body, headers, _ = self.signer.sign_query(query_body)
        return await self._post_body_with_headers(body, headers)

class _Network:
    def __init__(self, flureeclient, netname, options):
        self.client = flureeclient
        self.netname = netname
        self.options = options
    def __getitem__(self, key):
        database = self.netname + "/" + key
        if not key in self.options:
            raise KeyError("No such database: '" + database + "'")
        return _DbFunctor(self.client, database)

class _DbFunctor:
    def __init__(self, client, database):
        self.client = client
        self.database=database
    def __call__(self, privkey, auth_address, sig_validity=120, sig_fuel=1000):
        return _FlureeDbClient(privkey,
                              auth_address,
                              self.database,
                              self.client.host,
                              self.client.port,
                              self.client.https,
                              sig_validity,
                              sig_fuel,
                              self.client.session is None) 

class FlureeClient:
    """Basic asynchonous client for FlureeDB for non-database specific APIs"""
    def __init__(self,
                 privkey,
                 auth_address,
                 host="localhost",
                 port=8080,
                 https=False,
                 sig_validity=120,
                 sig_fuel=1000,
                 dryrun=False):
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
        self.signer = DbSigner(privkey, auth_address, None, sig_validity, sig_fuel)
        self.session = None
        if not dryrun:
            self.session = aiohttp.ClientSession()
        self.known_endpoints = set(["dbs","new_db","delete_db","add_server","remove_server","health","new_keys"])
        self.unsigned_endpoints = set(["dbs", "health", "new_keys"])
        self.use_get = set(["health"])
        self.required = dict()
        self.required["new_db"] = set(["db_id"])
        self.required["delete_db"] = set(["db_id"])
        self.required["add_server"] = set(["server"])
        self.required["delete_server"] = set(["server"])
        self.optional = {"new_db": set(["snapshot"])}
        self.implemented = set(["dbs", "new_keys", "health"])
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
        secure = ""
        if self.https:
            secure = "s"
        url = "http" + \
              secure + \
              "://" + \
              self.host + \
              ":" + \
              str(self.port) + \
              "/fdb/" + \
              api_endpoint
        signed = True
        if api_endpoint in self.unsigned_endpoints:
            signed = False
        use_get = False
        if api_endpoint in self.use_get:
            use_get = True
        required = set()
        if api_endpoint in self.required:
            required = self.required[api_endpoint]
        optional = set()
        if api_endpoint in self.optional:
            optional = self.optional[api_endpoint]
        if signed:
            return _SignedPoster(self.session, self.singer, url, required, optional)
        if use_get:
            return _UnsignedGetter(self.session, url)
        return _SignedPoster(self.session, self.signer, url, required, optional, unsigned=True)
    async def __getitem__(self, key):
        databases = await self.dbs()
        options = set()
        for pair in databases:
            if pair[0] == key:
                options.add(pair[1])
        if not bool(options):
            raise KeyError("No such network: '" + key + "'")
        return _Network(self, key, options)
    async def close_session(self):
        """Close HTTP(S) session to FlureeDB"""
        if self.session:
            await self.session.close()
        return



# pylint: disable=too-few-public-methods
class _FlureeDbClient:
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
                 sig_fuel=1000,
                 dryrun=False):
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
        self.session = None
        if not dryrun:
            self.session = aiohttp.ClientSession()
        self.known_endpoints = set(["snapshot","list_snapshots","export","query","multi_query","block","history","transact","graphql","sparql","command","reindex","hide","gen_flakes","query_with","test_transact_with","block_range_with","ledger_stats","storage","pw"])
        self.pw_endpoints = set(["generate","renew","login"])
        self.implemented = set(["query", "command"])

    async def close_session(self):
        """Close HTTP(S) session to FlureeDB"""
        if self.session:
            await self.session.close()
        return

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
                        The wrapping _FlureeDbClient
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
                if self.session:
                    async with self.session.post(self.url, data=body, headers=headers) as resp:
                        if resp.status != 200:
                            raise FlureeException(await resp.text())
                        return await resp.text()
                else:
                    print("url:", self.url)
                    print("########### HEADERS ############")
                    for key in headers:
                        print(key,":",headers[key])
                    print("############ BODY ##############")
                    print(body)
                    print("################################")
                    return('{"dryrun": true}')

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

            async def body_signed(self, transact_obj, deps=None):
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
                command = self.signer.sign_transaction(transact_obj, deps)
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
                        The wrapping _FlureeDbClient
                """
                self.stringendpoint = _StringEndpoint(api_endpoint, client)
            def __getattr__(self, method):
                if api_endpoint == "query":
                    return _FlureeQlQuery(self, method)
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
                        The wrapping _FlureeDbClient
                """
                self.stringendpoint = _StringEndpoint(api_endpoint, client)

            async def transaction(self, transaction_obj, deps=None):
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
                return await self.stringendpoint.body_signed(transaction_obj, deps)

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
