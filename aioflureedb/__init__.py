#!/usr/bin/python3
# pylint: disable=too-few-public-methods
# pylint: disable=too-many-arguments
# pylint: disable=too-many-instance-attributes
"""Basic asynchonous client library for FlureeDB"""
import sys
import asyncio
import json
import aiohttp
from aioflureedb.signing import DbSigner


class FlureeException(Exception):
    """Base exception class for aioflureedb"""
    def __init__(self, message):
        """Constructor

        Parameters
        ----------
        message : str
               Error message
        """
        Exception.__init__(self, message)


class FlureeHttpError(FlureeException):
    """Non 200 HTTP response"""
    def __init__(self, message, status):
        """Constructor

        Parameters
        ----------
        message : str
               Error message
        status : int
                 HTTP status code
        """
        self.status = status
        FlureeException.__init__(self, message)


class FlureeHalfCredentials(FlureeException):
    """Incomplete credentials"""
    def __init__(self, message):
        """Constructor

        Parameters
        ----------
        message : str
               Error message
        """
        FlureeException.__init__(self, message)


class FlureeKeyRequired(FlureeException):
    """Endpoint invoked that requires signing but no signing key available"""
    def __init__(self, message):
        """Constructor

        Parameters
        ----------
        message : str
               Error message
        """
        FlureeException.__init__(self, message)


class FlureeTransactionFailure(FlureeException):
    """Fluree transaction failed"""
    def __init__(self, message):
        """Constructor

        Parameters
        ----------
        message : str
               Error message
        """
        FlureeException.__init__(self, message)


def _dryrun(method, url, headers, body):
    """Helper function for debugging the library

    Parameters
    ----------
    method :  str
              HTTP method (POST or GET
    url :     str
              The url that would have been used
    headers : dict
              Dictionary with HTTP headers that would have been used
    body :    str
              The HTTP posting body
    Returns
    -------
    dict
        dummy dict, serves no purpose other than keeping to APIs
    """
    print("################################")
    print("#            dryrun            #")
    print("################################")
    print("             ", method)
    print("################################")
    print("    ", url)
    if headers is not None:
        print("########### HEADERS ############")
        for key in headers:
            print(key, ":", headers[key])
    if body is not None:
        print("############ BODY ##############")
        print(body)
        print("################################")
    return {"dryrun": True}


_FLUREEQLQUERY_ENDPOINT_PERMISSIONS = {
    'query': {
        'permitted': {"select", "selectOne", "selectDistinct", "from", "where", "block", "prefixes", "vars", "opts"},
        'depricated': {"filter", "union", "optional", "limit", "offset", "orderBy", "groupBy", "prettyPrint"}
    },
    'block': {
        'permitted': {"block"},
        'depricated': {'prettyPrint'}
    },
    'list_snapshots': {
        'permitted': {},
        'depricated': {}
    },
    'snapshot': {
        'permitted': {},
        'depricated': {}
    }
}


class _FlureeQlQuery:
    """Helper class for FlureeQL query syntactic sugar"""
    def __init__(self, endpoint):
        """Constructor

        Parameters
        ----------
        endpoint : _FlureeQlEndpoint
                   API endpoint for communicating FlureeQL queries with FlureeDB
        """
        self.endpoint = endpoint
        self.permittedkeys = _FLUREEQLQUERY_ENDPOINT_PERMISSIONS[endpoint.api_endpoint]['permitted']
        self.depricatedkeys = _FLUREEQLQUERY_ENDPOINT_PERMISSIONS[endpoint.api_endpoint]['depricated']

    async def __call__(self, **kwargs):
        """FlureeQl query construction through keyword arguments

        Parameters
        ----------
        kwargs: dict
                Keyword arguments for different parts of a FlureeQL query.

        Raises
        ------
        TypeError
            If an unknown kwarg value is used.

        Returns
        -------
        dict
            json decode result from the server.
        """
        obj = dict()
        for key, value in kwargs.items():
            if key == "ffrom":
                key = "from"
            if key == "ffilter":
                key = "filter"
            if key not in self.permittedkeys:
                if key not in self.depricatedkeys:
                    raise TypeError("FlureeQuery got unexpected keyword argument '" + key + "'")
                print("WARNING: Use of depricated FlureeQL syntax,",
                      key,
                      "should not be used as top level key in queries",
                      file=sys.stderr)
            obj[key] = value
        return await self.endpoint.actual_query(obj)

    async def raw(self, obj):
        """Use a readily constructed FlureeQL dictionary object to invoke the query API endpoint.

        Parameters
        ----------
        obj : Complete FlureeQl query object.

        Returns
        -------
        dict
            json decode result from the server.
        """
        return await self.endpoint.actual_query(obj)


class _UnsignedGetter:
    """Get info with a GET instead of a POST"""
    def __init__(self, session, url, ssl_verify_disabled=False, ready=None):
        """Constructor

        Parameters
        ----------
        session : aiohttp.ClientSession
                  HTTP session for doing HTTP post/get with
        url : string
              URL of the API endpoint.
        ssl_verify_disabled: bool
              If https, don't verify ssl certs
        ready : string
              If defined, provide a ready method to wait for ready condition to become true.
        """
        self.session = session
        self.url = url
        self.ssl_verify_disabled = ssl_verify_disabled
        self.ready_field = ready

    async def __call__(self):
        """Invoke the functor

        Returns
        -------
        dict
            JSON decoded response from the server

        Raises
        ------
        FlureeHttpError
            If the server returns something different than a 200 OK status
        """
        if self.session:
            if self.ssl_verify_disabled:
                async with self.session.get(self.url, ssl=False) as resp:
                    if resp.status != 200:
                        raise FlureeHttpError(await resp.text(), resp.status)
                    response = await resp.text()
                    return json.loads(response)
            else:
                async with self.session.get(self.url) as resp:
                    if resp.status != 200:
                        raise FlureeHttpError(await resp.text(), resp.status)
                    response = await resp.text()
                    return json.loads(response)
        else:
            return _dryrun("GET", self.url, None, None)

    async def ready(self):
        """Redo get untill ready condition gets met"""
        if self.ready_field is None:
            print("WARNING: no ready for this endpoint", file=sys.stderr)
            return
        while True:
            try:
                obj = await self()
                if obj[self.ready_field]:
                    return
            except FlureeHttpError:
                pass
            except aiohttp.client_exceptions.ClientConnectorError:
                pass
            await asyncio.sleep(0.5)


class _SignedPoster:
    """Basic signed HTTP posting"""
    def __init__(self, client, session, signer, url, required, optional, ssl_verify_disabled, unsigned=False):
        """Constructor

        Parameters
        ----------
        client : FlureeClient
            FlureeClient used for checking for new databases
        session : aiohttp.ClientSession
            HTTP session for doing HTTP post/get with
        signer : aioflureedb.signing.DbSigner
            ECDSA signer for Fluree transactions and queries
        url : string
            URL of the API endpoint
        required : set
            Set of required fields for the specific API call.
        optional : set
            Set of optional fields for the specific API call.
        ssl_verify_disabled: bool
            If https, ignore ssl certificate issues.
        unsigned : bool
            If True, don't sign posts.
        """
        self.client = client
        self.session = session
        self.signer = signer
        self.url = url
        self.required = required
        self.optional = optional
        self.unsigned = unsigned
        if self.signer is None:
            self.unsigned = True
        self.ssl_verify_disabled = ssl_verify_disabled

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
            Content as returned by HTTP server, dict if decodable json

        Raises
        ------
        FlureeHttpError
            When Fluree server returns a status code other than 200
        """
        if self.session:
            if self.ssl_verify_disabled:
                async with self.session.post(self.url, data=body, headers=headers, ssl=False) as resp:
                    if resp.status != 200:
                        raise FlureeHttpError(await resp.text(), resp.status)
                    data = await resp.text()
                    try:
                        return json.loads(data)
                    except json.decoder.JSONDecodeError:
                        return data
            else:
                async with self.session.post(self.url, data=body, headers=headers) as resp:
                    if resp.status != 200:
                        raise FlureeHttpError(await resp.text(), resp.status)
                    data = await resp.text()
                    try:
                        return json.loads(data)
                    except json.decoder.JSONDecodeError:
                        return data
        else:
            return json.loads(_dryrun("POST", self.url, headers, body))

    async def __call__(self, **kwargs):
        """Invoke post API

        Parameters
        ----------
        kwargs : dict
                 Keyword arguments for the POST API call.

        Returns
        -------
        dict
            JSON decoded response from FlureeDB server

        Raises
        ------
        TypeError
            If an unknown kwarg is used on invocation OR a required kwarg is not supplied
        """
        kwset = set()
        kwdict = dict()
        for key, value in kwargs.items():
            if not (key in self.required or key in self.optional):
                raise TypeError("SignedPoster got unexpected keyword argument '" + key + "'")
            kwset.add(key)
            if key == "db_id":
                kwdict["db/id"] = value
            else:
                kwdict[key] = value
        for reqkey in self.required:
            if reqkey not in kwset:
                raise TypeError("SignedPoster is missing one required named argument '", reqkey, "'")
        body = json.dumps(kwdict, indent=4, sort_keys=True)
        headers = {"Content-Type": "application/json"}
        if not self.unsigned:
            body, headers, _ = self.signer.sign_query(kwdict)
        rval = await self._post_body_with_headers(body, headers)
        # If this is a new-db, we need to await till it comes into existance.
        if isinstance(rval, str) and len(rval) == 64 and self.url.split("/")[-1] == "new-db" and "db_id" in kwargs:
            dbid = kwargs["db_id"]
            while True:
                databases = await self.client.dbs()
                for database in databases:
                    dbid2 = database[0] + "/" + database[1]
                    if dbid == dbid2:
                        return True
                await asyncio.sleep(0.1)
        return rval


class _Network:
    """Helper class for square bracket interface to Fluree Client"""
    def __init__(self, flureeclient, netname, options):
        """Constructor

        Parameters
        ----------
        flureeclient : FlureeClient
                       FlureeClient object to use as reference.
        netname : string
                  Name of the network for net/db fluree database naming.
        options : set
                  Set with existing databases within network.
        """
        self.client = flureeclient
        self.netname = netname
        self.options = options

    def __str__(self):
        """Cast to string

        Returns
        -------
        str
            Name of the network
        """
        return self.netname

    def __getitem__(self, key):
        """Square brackets operator

        Parameters
        ----------
        key : string
              Name of the desired database

        Returns
        -------
        _DbFunctor
            Function for constructing a Fluree Database client.

        Raises
        ------
        KeyError
            When a non defined database is requested.
        """
        database = self.netname + "/" + key
        if key not in self.options:
            raise KeyError("No such database: '" + database + "'")
        return _DbFunctor(self.client, database)

    def __iter__(self):
        """Iterate over databases in network

        Yields
        ------
        string
            Name of the database
        _DbFunctor
            Function object for getting a FlureeDB database object for this particular DB.
        """
        for key in self.options:
            database = self.netname + "/" + key
            yield _DbFunctor(self.client, database)


class _DbFunctor:
    """Helper functor class for square bracket interface to Fluree Client"""
    def __init__(self, client, database):
        """Constructor

        Parameters
        ----------
        client : FlureeClient
                 FlureeClient object to use as reference.
        database : string
                   Full database name
        """
        self.client = client
        self.database = database

    def __str__(self):
        """Cast to string

        Returns
        -------
        str
            Database name
        """
        return self.database

    def __call__(self, privkey=None, auth_address=None, sig_validity=120, sig_fuel=1000):
        """Invoke functor

        Parameters
        ----------
        privkey : string
                  Private key for the specific DB.
        auth_address : string
                  Auth ID belonging with the privkey
        sig_validity : int
                       Validity in seconda of signatures.
        sig_fuel : int
                   Not sure what this is for, consult FlureeDB documentation for info.
        Returns
        -------
         _FlureeDbClient
            FlureeClient derived client for a specific DB
        """
        return _FlureeDbClient(privkey,
                               auth_address,
                               self.database,
                               self.client.host,
                               self.client.port,
                               self.client.https,
                               self.client.ssl_verify,
                               sig_validity,
                               sig_fuel,
                               self.client.session is None)


class FlureeClient:
    """Basic asynchonous client for FlureeDB for non-database specific APIs"""
    def __init__(self,
                 masterkey=None,
                 auth_address=None,
                 host="localhost",
                 port=8080,
                 https=False,
                 ssl_verify=True,
                 sig_validity=120,
                 sig_fuel=1000,
                 dryrun=False):
        """Constructor

        Parameters
        ----------
        masterkey : string
                  Hex or base58 encoded signing key
        auth_address : string
                       key-id of the signing key
        host : string
                   hostname of the FlureeDB server. Defaults to localhost.
        port : int
                   port of the FlureeDB server. Defaults to 8080
        https : bool
                   Boolean indicating flureeDB is running behind a HTTPS proxy
        ssl_verify : bool
                   Boolean, if False, indicating to not verify ssl certs.
        sig_validity : int
                   Validity in seconda of the signature.
        sig_fuel : int
                   Not sure what this is for, consult FlureeDB documentation for info.
        dryrun : bool
                  Don't use HTTP, simply print queries/transactions instead

        Raises
        ------
        FlureeHalfCredentials
            If masterkey is specified but auth_address isn't, or the other way around.
        """
        self.host = host
        self.port = port
        self.https = https
        self.ssl_verify = ssl_verify
        self.ssl_verify_disabled = False
        if https and not ssl_verify:
            self.ssl_verify_disabled = True
        self.signer = None
        if masterkey and auth_address:
            self.signer = DbSigner(masterkey, auth_address, None, sig_validity, sig_fuel)
        if masterkey and not auth_address or auth_address and not masterkey:
            raise FlureeHalfCredentials("masterkey and auth_address should either both be specified, or neither")
        self.session = None
        if not dryrun:
            self.session = aiohttp.ClientSession()
        self.known_endpoints = set(["dbs",
                                    "new_db",
                                    "delete_db",
                                    "add_server",
                                    "remove_server",
                                    "health",
                                    "new_keys"])
        self.unsigned_endpoints = set(["dbs", "health", "new_keys"])
        self.use_get = set(["health", "new_keys"])
        self.required = dict()
        self.required["new_db"] = set(["db_id"])
        self.required["delete_db"] = set(["db_id"])
        self.required["add_server"] = set(["server"])
        self.required["delete_server"] = set(["server"])
        self.optional = {"new_db": set(["snapshot"])}
        self.implemented = set(["dbs",
                                "new_keys",
                                "health",
                                "new_db",
                                "delete_db",
                                "new_keys",
                                "add_server",
                                "remove_server"])

    async def __aenter__(self):
        """Method for allowing 'with' constructs

        Returns
        -------
        FlureeClient
            this fluree client
        """
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.close_session()

    def __dir__(self):
        """Dir function for class

        Returns
        -------
        list
            List of defined (pseudo) attributes
        """
        return list(self.known_endpoints) + ["close_session",
                                             "__init__",
                                             "__dir__",
                                             "__getattr__",
                                             "__getitem__",
                                             "__aiter__",
                                             " __aenter__",
                                             " __aexit__"]

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

        Raises
        ------
        AttributeError
            When a non-defined fluree endpoint is designated
        NotImplementedError
            When a fluree API endpoint is designated that hasn't been implemented yet.
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
              "-".join(api_endpoint.split("_"))
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
            return _SignedPoster(self, self.session, self.signer, url, required, optional, self.ssl_verify_disabled)
        if use_get:
            if api_endpoint == "health":
                return _UnsignedGetter(self.session, url, self.ssl_verify_disabled, ready="ready")
            return _UnsignedGetter(self.session, url, self.ssl_verify_disabled)
        return _SignedPoster(self, self.session, self.signer, url, required, optional, self.ssl_verify_disabled, unsigned=True)

    async def __getitem__(self, key):
        """Square bracket operator

        Parameters
        ----------
        key : string
              Network name, should be defined on server.

        Raises
        ------
        KeyError
            When a non-defined network is designated.

        Returns
        -------
        _Network
            Helper object for designating databases within a network.
        """
        subkey = None
        if "/" in key:
            parts = key.split("/")
            key = parts[0]
            subkey = parts[1]
        databases = await self.dbs()
        options = set()
        for pair in databases:
            if pair[0] == key:
                options.add(pair[1])
        if not bool(options):
            raise KeyError("No such network: '" + key + "'")
        network = _Network(self, key, options)
        if subkey is None:
            return network
        return network[subkey]

    async def __aiter__(self):
        """Iterate over all networks

        Yields
        ------
        string
            Name of the network
        _Network
            Itteratable object with databases per network.
        """
        databases = await self.dbs()
        optionsmap = dict()
        for pair in databases:
            network = pair[0]
            database = pair[1]
            if network not in optionsmap:
                optionsmap[network] = set()
            optionsmap[network].add(database)
        for key in optionsmap:
            yield _Network(self, key, optionsmap[key])

    async def close_session(self):
        """Close HTTP(S) session to FlureeDB"""
        if self.session:
            await self.session.close()
        return


class _FlureeDbClient:
    """Basic asynchonous client for FlureeDB representing a particular database on FlureeDB"""
    def __init__(self,
                 privkey,
                 auth_address,
                 database,
                 host="localhost",
                 port=8080,
                 https=False,
                 ssl_verify=True,
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
        ssl_verify : bool
                   Boolean, when false, indicating no validation of ssl certs.
        sig_validity : int
                   Validity in seconda of the signature.
        sig_fuel : int
                   Not sure what this is for, consult FlureeDB documentation for info.
        dryrun : bool
                 Don't use HTTP, simply print queries/transactions instead
        """
        self.database = database
        self.host = host
        self.port = port
        self.https = https
        self.ssl_verify_disabled = False
        self.monitor = dict()
        self.monitor["listeners"] = dict()
        self.monitor["running"] = False
        self.monitor["next"] = None
        self.monitor["rewind"] = 0
        self.monitor["on_block_processed"] = None
        self.monitor["predicate_map"] = dict()
        if https and not ssl_verify:
            self.ssl_verify_disabled = True
        self.signer = None
        if privkey and auth_address:
            self.signer = DbSigner(privkey, auth_address, database, sig_validity, sig_fuel)
        self.session = None
        if not dryrun:
            self.session = aiohttp.ClientSession()
        self.known_endpoints = set(["snapshot",
                                    "list_snapshots",
                                    "export",
                                    "query",
                                    "flureeql",
                                    "multi_query",
                                    "block",
                                    "history",
                                    "transact",
                                    "graphql",
                                    "sparql",
                                    "command",
                                    "reindex",
                                    "hide",
                                    "gen_flakes",
                                    "query_with",
                                    "test_transact_with",
                                    "block_range_with",
                                    "ledger_stats",
                                    "storage",
                                    "pw"])
        self.pw_endpoints = set(["generate", "renew", "login"])
        self.implemented = set(["query", "flureeql", "block", "command", "ledger_stats", "list_snapshots", "snapshot"])

    def monitor_init(self, on_block_processed, start_block=None, rewind=0):
        """Set the basic variables for a fluree block event monitor run

        Parameters
        ----------
        on_block_processed: callable
                Callback to invoke when a block has been fully processed.

        start_block: int
                Block number to start at, instead of the next block to arive on the blockchain

        rewind: int
                Number of seconds to rewind from now. Currently not implemented.

        Raises
        ------
        NotImplementedError
            Currently raised when rewind is specified.

        """

        assert callable(on_block_processed)
        assert start_block is None or isinstance(start_block, int)
        assert isinstance(rewind, int)
        if rewind != 0:
            raise NotImplementedError("rewind is not yet implemented")
        self.monitor["next"] = start_block
        self.monitor["rewind"] = rewind
        self.monitor["on_block_processed"] = on_block_processed

    def monitor_register_create(self, collection, callback):
        """Add a callback for create events on a collection

        Parameters
        ----------
        collection: str
                Name of the collection to monitor

        callback: callable
                Callback to invoke when create event on collection is identified.

        """
        assert isinstance(collection, str)
        assert callable(callback)
        if collection not in self.monitor["listeners"]:
            self.monitor["listeners"][collection] = dict()
        self.monitor["listeners"][collection]["C"] = set()
        self.monitor["listeners"][collection]["C"].add(callback)

    def monitor_register_delete(self, collection, callback):
        """Add a callback for delete events on a collection

        Parameters
        ----------
        collection: str
                Name of the collection to monitor

        callback: callable
                Callback to invoke when delete event on collection is identified.

        """
        assert isinstance(collection, str)
        assert callable(callback)
        if collection not in self.monitor["listeners"]:
            self.monitor["listeners"][collection] = dict()
        self.monitor["listeners"][collection]["D"] = set()
        self.monitor["listeners"][collection]["D"].add(callback)

    def monitor_register_update(self, collection, callback, predicates=None):
        """Add a callback for update events on a collection

        Parameters
        ----------
        collection: str
                Name of the collection to monitor

        callback: callable
                Callback to invoke when update event on collection is identified.

        predicates: list
                List of predicates. If defined, at laest one of these predicates should have been set or updated.

        Raises
        ------
        NotImplementedError
            Currently raised when predicates is specified.

        """

        assert isinstance(collection, str)
        assert callable(callback)
        assert predicates is None or isinstance(predicates, list)
        if isinstance(predicates, list):
            assert bool(predicates)
            for predicate in predicates:
                assert isinstance(predicate, str)
            raise NotImplementedError("predicates not yet implemented for monitor_register_update")
        if collection not in self.monitor["listeners"]:
            self.monitor["listeners"][collection] = dict()
        self.monitor["listeners"][collection]["U"] = dict()
        self.monitor["listeners"][collection]["U"]["callback"] = set()
        self.monitor["listeners"][collection]["U"]["callback"].add(callback)
        self.monitor["listeners"][collection]["U"]["predicates"] = predicates

    def monitor_close(self):
        """Abort running any running monitor"""
        self.monitor["running"] = False

    async def monitor_untill_stopped(self):
        """Run the block event monitor untill stopped

        Raises
        ------
        NotImplementedError
            Currently raised when rewing is specified.

        RuntimeError
            Raised either when there are no listeners set, or if there are too many errors.

        """
        # TODO: fix these when we look at getting rid of the extra queries.
        # pylint: disable = too-many-nested-blocks, too-many-locals, too-many-return-statements, too-many-branches, too-many-statements
        # Some basic asserts
        if not bool(self.monitor["listeners"]):
            raise RuntimeError("Can't start monitor with zero registered listeners")
        if self.monitor["rewind"] != 0:
            raise NotImplementedError("rewind is not implemented yet!")
        # Set running to true. We shall abort when it is set to false.
        self.monitor["running"] = True
        # First make a dict from the _predicate collection.
        predicates = await self.flureeql.query(select=["id", "name"], ffrom="_predicate")
        if not self.monitor["running"]:
            return
        predicate = dict()
        for pred in predicates:
            predicate[pred["_id"]] = pred["name"]
        noblocks = True
        if self.monitor["next"] is None:
            stats = await self.ledger_stats()
            if not self.monitor["running"]:
                return
            if "status" in stats and stats["status"] == 200 and "data" in stats and "block" in stats["data"]:
                startblock = stats["data"]["block"]
            else:
                raise RuntimeError("Invalid initial response from ledger_stats")
        else:
            startblock = self.monitor["next"]
        stats_error_count = 0
        while self.monitor["running"]:
            # If we had zero blocks to process the last time around, wait a full second before polling again if there are
            #  new blocks.
            if noblocks:
                await asyncio.sleep(1)
                if not self.monitor["running"]:
                    return
            noblocks = True
            # Get the latest ledger stats.
            stats = await self.ledger_stats()
            if not self.monitor["running"]:
                return
            if "status" in stats and stats["status"] == 200 and "data" in stats and "block" in stats["data"]:
                stats_error_count = 0
                endblock = stats["data"]["block"]
                if endblock > startblock:
                    noblocks = False
                    for block in range(startblock + 1, endblock + 1):
                        grouped = dict()
                        # Fetch the new block
                        block_data = await self.block.query(block=block)
                        if not self.monitor["running"]:
                            return
                        # Itterate all flakes.
                        for flake in block_data[0]["flakes"]:
                            predno = flake[1]
                            # Patch numeric predicates to textual ones.
                            if predno in predicate:
                                flake[1] = predicate[predno]
                            else:
                                raise RuntimeError("Need a restart after new predicates are added to the database")
                            # Group the flakes together by object.
                            if not flake[0] in grouped:
                                grouped[flake[0]] = list()
                            grouped[flake[0]].append(flake)
                        # Process per object.
                        for obj in grouped:
                            # Ectract the collection name
                            collection = grouped[obj][0][1].split("/")[0]
                            # Trigger on collection if in map
                            if collection in self.monitor["listeners"]:
                                latest = await self.flureeql.query(select=["*"], ffrom=obj)
                                if not self.monitor["running"]:
                                    return
                                if latest:
                                    latest = latest[0]
                                else:
                                    latest = None
                                previous = await self.flureeql.query(select=["*"], ffrom=obj, block=block-1)
                                if not self.monitor["running"]:
                                    return
                                if previous:
                                    previous = previous[0]
                                else:
                                    previous = None
                                if latest is None:
                                    if "D" in self.monitor["listeners"][collection]:
                                        for callback in self.monitor["listeners"][collection]["D"]:
                                            await callback(obj, grouped[obj])
                                            if not self.monitor["running"]:
                                                return
                                elif previous is None:
                                    if "C" in self.monitor["listeners"][collection]:
                                        for callback in self.monitor["listeners"][collection]["C"]:
                                            await callback(obj, grouped[obj])
                                            if not self.monitor["running"]:
                                                return
                                elif "U" in self.monitor["listeners"][collection]:
                                    for updateinfo in self.monitor["listeners"][collection]["U"]:
                                        await updateinfo["callback"](obj, grouped[obj])
                                        if not self.monitor["running"]:
                                            return
                        # Call the persistence layer.
                        await self.monitor["on_block_processed"](block)
                    # Set the new start block.
                    startblock = block
            else:
                stats_error_count += 1
                if stats_error_count > 100:
                    raise RuntimeError("Too many errors from ledger_stats call")

    async def ready(self):
        """Awaitable that polls the database untill the schema contains collections"""
        while True:
            try:

                await self.flureeql.query(
                    select=["_collection/name"],
                    ffrom="_collection"
                )
                return
            except FlureeHttpError:
                await asyncio.sleep(0.1)

    async def __aexit__(self, exc_type, exc, traceback):
        await self.close_session()

    async def __aenter__(self):
        """Method for allowing 'with' constructs

        Returns
        -------
        _FlureeDbClient
            this fluree DB client
        """
        return self

    async def close_session(self):
        """Close HTTP(S) session to FlureeDB"""
        if self.session:
            await self.session.close()
        return

    def __dir__(self):
        """Dir function for class

        Returns
        -------
        list
            List of defined (pseudo) attributes
        """
        return list(self.known_endpoints) + ["close_session",
                                             "__init__",
                                             "__dir__",
                                             "__getattr__",
                                             " __aenter__",
                                             " __aexit__"]

    def __getattr__(self, api_endpoint):
        # pylint: disable=too-many-statements
        """Select API endpoint

        Parameters
        ----------
        api_endpoint : string
                     Name of the API endpoint.

        Returns
        -------
        object
            Endpoint object suitable for API endpoint.

        Raises
        ------
        NotImplementedError
            Defined endpoint without library implementation (for now)
        AttributeError
            Undefined API endpoint invoked
        FlureeKeyRequired
            When 'command' endpoint is invoked in open-API mode.
        """
        class _StringEndpoint:
            def __init__(self, api_endpoint, client, ssl_verify_disabled=False):
                """Constructor

                Parameters
                ----------
                api_endpoint : string
                               Name of the API endpoint
                client: object
                        The wrapping _FlureeDbClient
                ssl_verify_disabled: bool
                    If https, dont validate ssl certs.
                """
                self.api_endpoint = api_endpoint
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
                           "-".join(api_endpoint.split("_"))
                self.signer = client.signer
                self.session = client.session
                self.ssl_verify_disabled = ssl_verify_disabled

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

                Raises
                ------
                FlureeHttpError
                    When HTTP status from fluree server is anything other than 200
                """
                if self.session:
                    if self.ssl_verify_disabled:
                        async with self.session.post(self.url, data=body, headers=headers, ssl=False) as resp:
                            if resp.status != 200:
                                raise FlureeHttpError(await resp.text(), resp.status)
                            return await resp.text()
                    else:
                        async with self.session.post(self.url, data=body, headers=headers) as resp:
                            if resp.status != 200:
                                raise FlureeHttpError(await resp.text(), resp.status)
                            return await resp.text()
                else:
                    return json.loads(_dryrun("POST", self.url, headers, body))

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
                if self.signer:
                    body, headers, _ = self.signer.sign_query(query_body, querytype=self.api_endpoint)
                else:
                    body = json.dumps(query_body, indent=4, sort_keys=True)
                    headers = {"Content-Type": "application/json"}
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

            async def empty_post_unsigned(self):
                """Do an HTTP POST without body and without signing

                Returns
                -------
                string
                    Return body from server
                """
                return await self._post_body_with_headers(None, None)

        class FlureeQlEndpoint:
            """Endpoint for JSON based (FlureeQl) queries"""
            def __init__(self, api_endpoint, client, ssl_verify_disabled):
                """Constructor

                Parameters
                ----------
                api_endpoint : string
                               Name of the API endpoint
                client: object
                        The wrapping _FlureeDbClient
                ssl_verify_disabled: bool
                    When using https, don't validata ssl certs.
                """
                if api_endpoint == "flureeql":
                    api_endpoint = "query"

                self.api_endpoint = api_endpoint
                self.stringendpoint = _StringEndpoint(api_endpoint, client, ssl_verify_disabled)

            def __dir__(self):
                """Dir function for class

                Returns
                -------
                list
                    List of defined (pseudo) attributes
                """
                return ["query", "actual_query", "__dir__", "__init__"]

            def __getattr__(self, method):
                """query

                Parameters
                ----------
                method : string
                         should be 'query'

                Returns
                -------
                _FlureeQlQuery
                    Helper class for creating FlureeQl queries.

                Raises
                ------
                AttributeError
                    When anything other than 'query' is provided as method.
                """
                if method != 'query':
                    raise AttributeError("FlureeQlEndpoint has no attribute named " + method)
                return _FlureeQlQuery(self)

            async def actual_query(self, query_object):
                """Execure a query with a python dict that should get JSON serialized and convert JSON
                   response back into a python object

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

        class CommandEndpoint:
            """Endpoint for FlureeQL command"""
            def __init__(self, api_endpoint, client):
                """Constructor

                Parameters
                ----------
                api_endpoint : string
                               Name of the API endpoint
                client: object
                        The wrapping _FlureeDbClient
                """
                self.client = client
                self.stringendpoint = _StringEndpoint(api_endpoint, client)

            async def transaction(self, transaction_obj, deps=None, do_await=True):
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

                Raises
                ------
                FlureeTransactionFailure
                    When transaction fails
                """
                tid = await self.stringendpoint.body_signed(transaction_obj, deps)
                tid = tid[1:-1]
                if not do_await:
                    return tid
                while True:
                    status = await self.client.query.query(select=["*"], ffrom=["_tx/id", tid])
                    if status:
                        if "error" in status[0]:
                            raise FlureeTransactionFailure("Transaction failed:" + status[0]["error"])
                        if "_tx/error" in status[0]:
                            raise FlureeTransactionFailure("Transaction failed:" + status[0]["_tx/error"])
                        return status[0]
                    await asyncio.sleep(0.1)

        class LedgerStatsEndpoint:
            """Endpoint for ledger_stats"""
            def __init__(self, client):
                """Constructor

                Parameters
                ----------
                client: object
                        The wrapping _FlureeDbClient
                """
                self.stringendpoint = _StringEndpoint('ledger_stats', client)

            async def __call__(self):
                """Send request to ledger-stats endpoint and retrieve result

                Returns
                -------
                dict
                    json decode result from the server.
                """
                return_body = await self.stringendpoint.empty_post_unsigned()
                return json.loads(return_body)

        if api_endpoint not in self.known_endpoints:
            raise AttributeError("FlureeDB has no endpoint named " + api_endpoint)
        if api_endpoint not in self.implemented:
            raise NotImplementedError("No implementation yet for " + api_endpoint)
        if api_endpoint in ["command"]:
            if self.signer is None:
                raise FlureeKeyRequired("Command endpoint not supported in open-API mode. privkey required!")
            return CommandEndpoint(api_endpoint, self)
        if api_endpoint == 'ledger_stats':
            return LedgerStatsEndpoint(self)
        return FlureeQlEndpoint(api_endpoint, self, self.ssl_verify_disabled)
