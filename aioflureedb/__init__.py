#!/usr/bin/python3
# pylint: disable=too-few-public-methods
# pylint: disable=too-many-arguments
# pylint: disable=too-many-instance-attributes
# pylint: disable=simplifiable-if-statement
"""Basic asynchonous client library for FlureeDB"""
import sys
import asyncio
import json
import time
import aiohttp
from aioflureedb.signing import DbSigner
from aioflureedb.domain_api import FlureeDomainAPI


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


class FlureeUnexpectedPredicateNumber(FlureeException):
    """Fluree transaction failed"""
    def __init__(self, message):
        """Constructor

        Parameters
        ----------
        message : str
               Error message
        """
        FlureeException.__init__(self, message)


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
    },
    'reindex': {
        'permitted': {},
        'depricated': {}
    },
    'history': {
        'permitted': {"history", "block", "showAuth"},
        'depricated': {}
    }
}


class _FlureeQlSubQuery:
    """Helper class for FlureeQL multi-query syntactic sugar"""
    def __init__(self, endpoint, method):
        """Constructor

        Parameters
        ----------
        endpoint : _FlureeQlEndpoint
                   API endpoint for communicating FlureeQL queries with FlureeDB
        method : str
               Name for the sub-query
        """
        self.endpoint = endpoint
        self.method = method
        self.permittedkeys = _FLUREEQLQUERY_ENDPOINT_PERMISSIONS["query"]['permitted']
        self.depricatedkeys = _FLUREEQLQUERY_ENDPOINT_PERMISSIONS["query"]['depricated']

    def __call__(self, **kwargs):
        """FlureeQl query construction through keyword arguments

        Parameters
        ----------
        kwargs: dict
                Keyword arguments for different parts of a FlureeQL query.

        Raises
        ------
        TypeError
            If an unknown kwarg value is used.

        """
        obj = {}
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
        self.endpoint.multi_query[self.method] = obj


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
        obj = {}
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
        obj: dict
            Complete FlureeQl query object.

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
                try:
                    rval = json.loads(response)
                except json.decoder.JSONDecodeError:
                    rval = response
                return rval

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
            except FlureeHttpError as ex:
                print(ex)
            except aiohttp.client_exceptions.ClientConnectorError:
                pass
            await asyncio.sleep(2)


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
        # pylint: disable=too-many-locals
        kwset = set()
        kwdict = {}
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

    def __call__(self, privkey=None, sig_validity=120, sig_fuel=1000):
        """Invoke functor

        Parameters
        ----------
        privkey : string
                  Private key for the specific DB.
        sig_validity : int
                       Validity in seconda of signatures.
        sig_fuel : int
                   Not sure what this is for, consult FlureeDB documentation for info.
        Returns
        -------
         _FlureeDbClient
            FlureeClient derived client for a specific DB
        """
        assert isinstance(sig_validity, (float, int))
        return _FlureeDbClient(privkey,
                               self.database,
                               self.client.host,
                               self.client.port,
                               self.client.https,
                               self.client.ssl_verify,
                               sig_validity,
                               sig_fuel)


class FlureeClient:
    """Basic asynchonous client for FlureeDB for non-database specific APIs"""
    def __init__(self,
                 masterkey=None,
                 host="localhost",
                 port=8080,
                 https=False,
                 ssl_verify=True,
                 sig_validity=120,
                 sig_fuel=1000):
        """Constructor

        Parameters
        ----------
        masterkey : string
                  Hex or base58 encoded signing key
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

        """
        assert isinstance(sig_validity, (float, int))
        self.host = host
        self.port = port
        self.https = https
        self.ssl_verify = ssl_verify
        self.ssl_verify_disabled = False
        if https and not ssl_verify:
            self.ssl_verify_disabled = True
        self.signer = None
        if masterkey:
            self.signer = DbSigner(masterkey, None, sig_validity, sig_fuel)
        self.session = None
        self.session = aiohttp.ClientSession()
        self.known_endpoints = set(["dbs",
                                    "new_db",
                                    "delete_db",
                                    "add_server",
                                    "remove_server",
                                    "health",
                                    "new_keys",
                                    "sub",
                                    "nw_state",
                                    "version"])
        self.unsigned_endpoints = set(["dbs", "health", "new_keys", "nw_state", "version"])
        self.use_get = set(["health", "new_keys", "nw_state", "version"])
        self.required = {}
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
                                "add_server",
                                "remove_server",
                                "nw_state",
                                "version"])

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
        optionsmap = {}
        for pair in databases:
            network = pair[0]
            database = pair[1]
            if network not in optionsmap:
                optionsmap[network] = set()
            optionsmap[network].add(database)
        for key, item in optionsmap.items():
            yield _Network(self, key, item)

    async def close_session(self):
        """Close HTTP(S) session to FlureeDB"""
        if self.session:
            await self.session.close()
        return


class _FlureeDbClient:
    """Basic asynchonous client for FlureeDB representing a particular database on FlureeDB"""
    def __init__(self,
                 privkey,
                 database,
                 host="localhost",
                 port=8080,
                 https=False,
                 ssl_verify=True,
                 sig_validity=120,
                 sig_fuel=1000):
        """Constructor

        Parameters
        ----------
        privkey : string
                  Hex or base58 encoded signing key
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
        """
        assert isinstance(sig_validity, (float, int))
        self.database = database
        self.host = host
        self.port = port
        self.https = https
        self.ssl_verify_disabled = False
        self.monitor = {}
        self.monitor["listeners"] = {}
        self.monitor["running"] = False
        self.monitor["next"] = None
        self.monitor["rewind"] = 0
        self.monitor["on_block_processed"] = None
        self.monitor["predicate_map"] = {}
        self.monitor["predicate_map_multi"] = {}
        self.monitor["predicate_map_block"] = 0
        self.monitor["lastblock_instant"] = None
        self.monitor["instant_monitors"] = []
        if https and not ssl_verify:
            self.ssl_verify_disabled = True
        self.signer = None
        if privkey:
            self.signer = DbSigner(privkey, database, sig_validity, sig_fuel)
        self.session = None
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
                                    "sql",
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
        self.implemented = set(["query",
                                "flureeql",
                                "sql",
                                "sparql",
                                "block",
                                "command",
                                "ledger_stats",
                                "list_snapshots",
                                "snapshot",
                                "multi_query",
                                "history",
                                "reindex"])

    def monitor_init(self, on_block_processed, start_block=None, rewind=0, always_query_object=False, start_instant=None):
        """Set the basic variables for a fluree block event monitor run

        Parameters
        ----------
        on_block_processed: callable
                Callback to invoke when a block has been fully processed.

        start_block: int
                Block number to start at, instead of the next block to arive on the blockchain

        rewind: int
                Number of seconds to rewind from now. Currently not implemented.

        always_query_object: bool
                Boolean choosing if we want to run efficiently and only query if block parsing gives ambiguous results,
                or if we always want to use extra queries

        start_instant: int
                If (and only if) instant monitor callbacks are used, this parameter should be provided to avoid
                large replays of inactive chain instant events that occured after the last block. Use the instant
                as provided py the persistence callback in the previous run.

        Raises
        ------
        NotImplementedError
            Currently raised when rewind is specified.

        """

        assert callable(on_block_processed)
        assert start_block is None or isinstance(start_block, int)
        assert isinstance(rewind, int)
        self.monitor["next"] = start_block
        self.monitor["rewind"] = rewind
        self.monitor["always_query_object"] = always_query_object
        self.monitor["on_block_processed"] = on_block_processed
        self.monitor["lastblock_instant"] = start_instant

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
            self.monitor["listeners"][collection] = {}
        if "C" not in self.monitor["listeners"][collection]:
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
            self.monitor["listeners"][collection] = {}
        if "D" not in self.monitor["listeners"][collection]:
            self.monitor["listeners"][collection]["D"] = set()
        self.monitor["listeners"][collection]["D"].add(callback)

    def monitor_register_update(self, collection, callback):
        """Add a callback for update events on a collection

        Parameters
        ----------
        collection: str
                Name of the collection to monitor

        callback: callable
                Callback to invoke when update event on collection is identified.

        """

        assert isinstance(collection, str)
        assert callable(callback)
        if collection not in self.monitor["listeners"]:
            self.monitor["listeners"][collection] = {}
        if "U" not in self.monitor["listeners"][collection]:
            self.monitor["listeners"][collection]["U"] = set()
        self.monitor["listeners"][collection]["U"].add(callback)

    def monitor_instant(self, predicate, callback, offset=0):
        """Ass a callback for the passing of time on an instant predicate

        Parameters
        ----------
        predicate: str
                Name of the instant predicate to monitor
        callback: callable
                Callback to invoke when the time (plus offset) passes the monitored instant
        offset: int
                If specified, number of seconds from monitored instant value to trigger on
        """
        self.monitor["instant_monitors"].append([predicate, offset*1000, callback])

    def monitor_close(self):
        """Abort running any running monitor"""
        self.monitor["running"] = False

    async def _figure_out_next_block(self):
        """Figure out what block the user wants/needs to be the next block"""
        if self.monitor["rewind"] != 0 and self.monitor["rewind"] is not None:
            filt = "(> ?instant (- (now) (* 1000 " + str(self.monitor["rewind"]) + "))))"
            rewind_block = await self.flureeql.query(
                select=["?blockid"],
                opts={"orderBy": ["ASC", "?instant"], "limit": 1},
                where=[
                    ["?block", "_block/instant", "?instant"],
                    ["?block", "_block/number", "?blockid"],
                    {"filter": [filt]}
                ])
            if rewind_block and (self.monitor["next"] is None or self.monitor["next"] < rewind_block[0][0]):
                self.monitor["next"] = rewind_block[0][0]
            if not rewind_block:
                self.monitor["next"] = None

    async def _build_predicates_map(self, block=None):
        """Build a predicates map for quick lookup

        Returns
        -------
        dict
            dictionary mapping predicate id's to predicate names
        """
        if block is not None:
            if self.monitor["predicate_map_block"] != block:
                predicates = await self.flureeql.query(select=["name", "multi"], ffrom="_predicate", block=block)
                self.monitor["predicate_map_block"] = block
            else:
                predicates = None
        else:
            predicates = await self.flureeql.query(select=["name", "multi"], ffrom="_predicate")
        if predicates is not None:
            predicate = {}
            is_multi = {}
            for pred in predicates:
                predicate[pred["_id"]] = pred["name"]
                if "multi" in pred:
                    is_multi[pred["name"]] = pred["multi"]
                else:
                    is_multi[pred["name"]] = False
            self.monitor["predicate_map"] = predicate
            self.monitor["predicate_map_multi"] = is_multi

    async def _find_start_block(self):
        """Find the start block

        Returns
        -------
        int
            Number of the starting block

        Raises
        ------
        RuntimeError
             Raised when the very first ledger_stats issued to FlureeDB returns an error.
        """
        if self.monitor["next"] is None:
            stats = await self.ledger_stats()
            if "status" in stats and stats["status"] == 200 and "data" in stats and "block" in stats["data"]:
                startblock = stats["data"]["block"]
            else:
                raise RuntimeError("Invalid initial response from ledger_stats")
        else:
            startblock = self.monitor["next"]
        return startblock

    async def _get_endblock(self, errorcount=0):
        """Get what for now should be the ending block

        Parameters
        ----------
        errorcount:  int
                     Counter for counting succesive API failure
        Returns
        -------
        int
            The ending block number
        int
            An updated version of the errorcount argument
        """
        stats = await self.ledger_stats()
        if "status" in stats and stats["status"] == 200 and "data" in stats and "block" in stats["data"]:
            endblock = stats["data"]["block"]
            return endblock, 0
        return 0, errorcount + 1

    def _get_flakeset_collection(self, flakelist):
        """Helper function for getting the collection name from a flakes array

        Parameters
        ----------
        flakelist :  list
              list of flake lists

        Returns
        -------
        str
            Name of the collection.
        """
        return flakelist[0][1].split("/")[0]

    async def _group_block_flakes(self, block_data, blockno):
        """Return a grouped-by object-id and predicate name patched version of a block

        Parameters
        ----------
        block_data :  list
              Raw block data as returned by FlureeDB
        predicate :  dict
              Dictionary for looking up predicate names by number
        blockno : int
              Number of the block currently being processed.

        Returns
        -------
        dict
            A dictionary of object id's to flake arrays.

        Raises
        ------
        FlureeUnexpectedPredicateNumber
            Raised when an unknown predicate id is detected.
        """
        has_predicate_updates = False
        grouped = {}
        for flake in block_data[0]["flakes"]:
            predno = flake[1]
            # Patch numeric predicates to textual ones.
            if predno in self.monitor["predicate_map"]:
                flake[1] = self.monitor["predicate_map"][predno]
            else:
                raise FlureeUnexpectedPredicateNumber("Need a restart after new predicates are added to the database")
            # Group the flakes together by object.
            if not flake[0] in grouped:
                grouped[flake[0]] = []
            grouped[flake[0]].append(flake)
        # pylint: disable=consider-using-dict-items
        for obj in grouped:
            if grouped[obj][0][1].split("/")[0] == "_predicate":
                has_predicate_updates = True
        # pylint: enable=consider-using-dict-items
        if has_predicate_updates:
            await self._build_predicates_map(blockno)
        return grouped

    def _get_transactions_and_temp_ids(self, flakeset):
        """Extract transactions and temp id's from a single 'tx' flakeset

        Parameters
        ----------
        flakeset :  list
              List of flakes belonging to a 'tx' in the current block.

        Returns
        -------
        list
            list of operations from this transaction
        dict
            map of temporary ids.
        """
        operations = None
        tempids = None
        for flake in flakeset:
            if flake[1] == '_tx/tempids':
                try:
                    tid_obj = json.loads(flake[2])
                    if isinstance(tid_obj, dict):
                        tempids = tid_obj
                except json.decoder.JSONDecodeError:
                    pass
            elif flake[1] == "_tx/tx":
                try:
                    tx_obj = json.loads(flake[2])
                    if isinstance(tx_obj, dict) and "tx" in tx_obj and isinstance(tx_obj["tx"], list):
                        operations = tx_obj["tx"]
                except json.decoder.JSONDecodeError:
                    pass
        return operations, tempids

    def _get_block_instant(self, flakeset):
        """Extract transactions and temp id's from a single 'tx' flakeset

        Parameters
        ----------
        flakeset :  list
              List of flakes belonging to a 'tx' in the current block.

        Returns
        -------
        int
            Time instance value for this block
        """
        instance = None
        for flake in flakeset:
            if flake[1] == '_block/instant':
                instance = flake[2]
        return instance

    async def _get_block_instant_by_blockno(self, block):
        """Get the instant timestamp for a given block number

        Parameters
        ----------
        block: int
            Block number

        Returns
        -------
        int
            Time instance value for this block
        """
        result = await self.flureeql.query(
            select=["?instant"],
            where=[
                ["?block", "_block/instant", "?instant"],
                ["?block", "_block/number", block]
            ]
        )
        if result:
            return result[0][0]
        return None

    def _get_object_id_to_operation_map(self, tempids, operations):
        """Process temp ids and operations, return an object id to operation map.

        Parameters
        ----------
        tempids :  dict
                  Temp-id map
        operations :     list
                  The url that would have been used

        Returns
        -------
        dict
            object id to operation map.
        """
        # pylint: disable=too-many-nested-blocks, too-many-branches
        obj_tx = {}
        if tempids:
            for tmp_id in tempids:
                real_id = tempids[tmp_id]
                counters = {}
                if isinstance(real_id, int):
                    for operation in operations:
                        if isinstance(operation, dict) and "_id" in operation:
                            if isinstance(operation["_id"], str):
                                if operation["_id"] == tmp_id:
                                    obj_tx[real_id] = operation
                                if operation["_id"] not in counters:
                                    counters[operation["_id"]] = 0
                                counters[operation["_id"]] += 1
                                altname = operation["_id"] + "$" + str(counters[operation["_id"]])
                                if altname == tmp_id:
                                    obj_tx[real_id] = operation
                            elif isinstance(operation["_id"], list):
                                if len(operation["_id"]) == 2:
                                    txid = '["' + operation["_id"][0] + '" "' + operation["_id"][1] + '"]'
                                    if txid == tmp_id:
                                        obj_tx[real_id] = operation
        if len(operations) == 1:
            obj_tx[""] = operations[0]
        for operation in operations:
            if isinstance(operation, dict) and "_id" in operation:
                if isinstance(operation["_id"], int):
                    obj_tx[operation["_id"]] = operation
        return obj_tx

    async def _do_instant_monitor(self, oldinstant, newinstant, blockno):
        for monitor in self.monitor["instant_monitors"]:
            predicate = monitor[0]
            offset = monitor[1]
            callback = monitor[2]
            windowstart = oldinstant - offset
            windowstop = newinstant - offset
            filt = "(and (> ?instant " + str(windowstart) + ") (<= ?instant " + str(windowstop) + "))"
            eventlist = await self.flureeql.query(
                select=[{"?whatever": ["*"]}],
                opts={"orderBy": ["ASC", "?instant"]},
                where=[
                    ["?whatever", predicate, "?instant"],
                    {"filter": [filt]}
                ],
                block=blockno
                )
            for event in eventlist:
                await callback(event)

    async def _process_instant(self, instant, block, fromblock):
        minute = 60000
        timeout = 1*minute
        if (fromblock or
                self.monitor["lastblock_instant"] and
                self.monitor["lastblock_instant"] + timeout < instant):
            if self.monitor["lastblock_instant"]:
                await self._do_instant_monitor(self.monitor["lastblock_instant"], instant, block)
            self.monitor["lastblock_instant"] = instant

    async def _get_and_preprocess_block(self, blockno):
        """Fetch a block by block number and preprocess it

        Parameters
        ----------
        blockno :  int
                  Number of the block that needs to be fetched
        predicate : dict
                  Predicate id to name map

        Returns
        -------
        list
            A grouped and predicate patched version of the fetched block.
        dict
            Object id to operation dict
        """
        # Fetch the new block
        block_data = await self.block.query(block=blockno)
        # Groub by object
        try:
            grouped = await self._group_block_flakes(block_data, blockno)
        except FlureeUnexpectedPredicateNumber:
            await self._build_predicates_map(blockno)
            grouped = await self._group_block_flakes(block_data, blockno)
        # Distill new ones using _tx/tempids
        obj_tx = {}
        block_meta = {}
        for obj in grouped:
            transactions = None
            tempids = None
            instant = None
            collection = self._get_flakeset_collection(grouped[obj])
            if collection == "_tx":
                transactions, tempids = self._get_transactions_and_temp_ids(grouped[obj])
            if collection == "_block":
                instant = self._get_block_instant(grouped[obj])
                for flake in grouped[obj]:
                    if len(flake[1].split("/")) > 1:
                        block_meta[flake[1].split("/")[1]] = flake[2]
            if transactions:
                obj_tx = self._get_object_id_to_operation_map(tempids, transactions)
            if instant:
                await self._process_instant(instant, blockno, True)
        return grouped, obj_tx, instant, block_meta

    async def _process_flakeset(self, collection, obj, obj_tx, blockno, block_meta):
        """Process temp ids and operations, return an object id to operation map.

        Parameters
        ----------
        collection :  str
                  name of the collection the object for this flakeset refers to
        obj :     list
                  The flakelist
        obj_tx :  dict
                  Dictionary mapping from object id to operation object.
        blockno : int
                  Block number of the block currently being processed.

        """
        # pylint: disable=too-many-branches,too-many-statements
        operation = None
        action = None
        previous = None
        latest = None
        if obj[0][0] in obj_tx:
            operation = obj_tx[obj[0][0]]
        elif "" in obj_tx:
            operation = obj_tx[""]
        has_true = False
        has_false = False
        has_multi = False
        for flake in obj:
            if flake[4]:
                has_true = True
            else:
                has_false = True
            if flake[1] in self.monitor["predicate_map_multi"]:
                if self.monitor["predicate_map_multi"][flake[1]]:
                    has_multi = True
        if self.monitor["always_query_object"]:
            previous = await self.flureeql.query(select=["*"], ffrom=obj[0][0], block=blockno-1)
            if previous:
                previous = previous[0]
                action = "update"
            else:
                previous = None
            latest = await self.flureeql.query(select=["*"], ffrom=obj[0][0], block=blockno)
            if latest:
                latest = latest[0]
            else:
                latest = None
            if previous is None:
                action = "insert"
            elif latest is None:
                action = "delete"
            else:
                action = "update"
        if operation and "_action" in operation and operation["_action"] != "upsert" and not has_multi:
            action = operation["_action"]
        if action is None and has_true and has_false:
            action = "update"
        if action is None and operation and "_id" in operation and isinstance(operation["_id"], str):
            action = "insert"
        if action is None and operation and has_false and not has_true:
            if len(obj) == 1 and has_multi:
                action = "update"
            else:
                action = "delete"
        if action is None and has_true and not has_false:
            if blockno > 1:
                previous = await self.flureeql.query(select=["*"], ffrom=obj[0][0], block=blockno-1)
                if previous:
                    previous = previous[0]
                    action = "update"
                else:
                    previous = None
                    action = "insert"
            else:
                previous = None
                action = "insert"
        if action is None:
            latest = await self.flureeql.query(select=["*"], ffrom=obj[0][0], block=blockno)
            if latest:
                latest = latest[0]
                action = "update"
            else:
                latest = None
                action = "delete"
        if action == "insert" and "C" in self.monitor["listeners"][collection]:
            for callback in self.monitor["listeners"][collection]["C"]:
                await callback(obj_id=obj[0][0], flakes=obj, new_obj=latest, operation=operation, block_meta=block_meta)
        elif action == "update" and "U" in self.monitor["listeners"][collection]:
            for callback in self.monitor["listeners"][collection]["U"]:
                await callback(obj_id=obj[0][0],
                               flakes=obj,
                               old_obj=previous,
                               new_obj=latest,
                               operation=operation,
                               block_meta=block_meta)
        elif action == "delete" and "D" in self.monitor["listeners"][collection]:
            for callback in self.monitor["listeners"][collection]["D"]:
                await callback(obj_id=obj[0][0], flakes=obj, old_obj=previous, operation=operation, block_meta=block_meta)

    async def monitor_untill_stopped(self):
        """Run the block event monitor untill stopped

        Raises
        ------
        NotImplementedError
            Currently raised when rewing is specified.

        RuntimeError
            Raised either when there are no listeners set, or if there are too many errors.

        """
        # pylint: disable=too-many-nested-blocks, too-many-branches, too-many-return-statements
        if (not bool(self.monitor["listeners"])) and (not bool(self.monitor["instant_monitors"])):
            raise RuntimeError("Can't start monitor with zero registered listeners")
        # Set running to true. We shall abort when it is set to false.
        self.monitor["running"] = True
        await self._figure_out_next_block()
        if not self.monitor["running"]:
            return
        startblock = await self._find_start_block() + 1
        if not self.monitor["running"]:
            return
        # First make a dict from the _predicate collection.
        if startblock > 1:
            await self._build_predicates_map(startblock - 1)
        if not self.monitor["running"]:
            return
        noblocks = True
        if startblock > 1 and self.monitor["instant_monitors"] and self.monitor["lastblock_instant"] is None:
            self.monitor["lastblock_instant"] = await self._get_block_instant_by_blockno(startblock-1)
        if not self.monitor["running"]:
            return
        stats_error_count = 0
        last_instant = 0
        while self.monitor["running"]:
            # If we had zero blocks to process the last time around, wait a full second before
            # polling again if there are new blocks.
            if noblocks:
                await asyncio.sleep(1)
                if not self.monitor["running"]:
                    return
                await self._process_instant(int(time.time()*1000), startblock - 1, False)
                now = int(time.time()*1000)
                if now - last_instant >= 59500:  # Roughly one minute
                    last_instant = now
                    await self.monitor["on_block_processed"](startblock - 1, now)
                if not self.monitor["running"]:
                    return

            noblocks = True
            endblock, stats_error_count = await self._get_endblock()
            if not self.monitor["running"]:
                return
            if endblock:
                if endblock >= startblock:
                    noblocks = False
                    for block in range(startblock, endblock + 1):
                        grouped, obj_tx, instant, block_meta = await self._get_and_preprocess_block(block)
                        # Process per object.
                        for obj in grouped:
                            if obj > 0:
                                collection = self._get_flakeset_collection(grouped[obj])
                                if collection in self.monitor["listeners"]:
                                    await self._process_flakeset(collection, grouped[obj], obj_tx, block, block_meta)
                                    if not self.monitor["running"]:
                                        return
                        # Call the persistence layer.
                        await self.monitor["on_block_processed"](block, instant)
                        last_instant = instant
                    # Set the new start block.
                    startblock = endblock + 1
            else:
                stats_error_count += 1
                if stats_error_count > 100:
                    raise RuntimeError("Too many errors from ledger_stats call")

    async def ready(self):
        """Awaitable that polls the database untill the schema contains collections

        Raises
        ------
        FlureeHttpError
            When the error from FlureeDB is db/invalid-auth
        """
        while True:
            try:

                await self.flureeql.query(
                    select=["_collection/name"],
                    ffrom="_collection"
                )
                return
            except FlureeHttpError as ex:
                result = json.loads(ex.args[0])
                if result["error"] == "db/invalid-auth":
                    raise ex
                await asyncio.sleep(2)

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

            async def header_signed(self, query_body, contenttype="application/json"):
                """Do a HTTP query using headers for signing

                Parameters
                ----------
                query_body : any
                       query body to sign using headers.
                contenttype : string
                       Content-type of query, defaults to application/json

                Returns
                -------
                string
                    Return body from server
                """
                if self.signer:
                    body, headers, _ = self.signer.sign_query(query_body, querytype=self.api_endpoint)
                else:
                    body = json.dumps(query_body, indent=4, sort_keys=True)
                    headers = {"Content-Type": contenttype}
                return await self._post_body_with_headers(body, headers)

            async def body_signed(self, transact_obj, deps=None):
                """Do a HTTP query using body envelope for signing
                Parameters
                ----------
                transact_obj : list
                       transaction to sign using body envelope.
                deps: dict
                    FlureeDb debs

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

        class FlureeQlEndpointMulti:
            """Endpoint for JSON based (FlureeQl) multi-queries"""
            def __init__(self, client, ssl_verify_disabled, raw=None):
                """Constructor

                Parameters
                ----------
                client: object
                        The wrapping _FlureeDbClient

                ssl_verify_disabled: bool
                    When using https, don't validata ssl certs.

                raw: dict
                    The whole raw multiquery
                """
                self.stringendpoint = _StringEndpoint("multi_query", client, ssl_verify_disabled)
                if raw:
                    self.multi_query = raw
                else:
                    self.multi_query = {}

            def __call__(self, raw=None):
                """Invoke as function object.

                Parameters
                ----------
                raw: dict
                    The whole raw multiquery

                Returns
                -------
                FlureeQlEndpointMulti
                    Pointer to self
                """
                if raw is not None:
                    self.multi_query = raw
                return self

            def __dir__(self):
                """Dir function for class

                Returns
                -------
                list
                    List of defined (pseudo) attributes
                """
                return ["__call__", "__dir__", "__init__"]

            def __getattr__(self, method):
                """query

                Parameters
                ----------
                method : string
                         subquery name

                Returns
                -------
                _FlureeQlSubQuery
                    Helper class for creating FlureeQl multi-queries.

                """
                return _FlureeQlSubQuery(self, method)

            async def query(self):
                """Do the actual multi-query

                Returns
                -------
                dict
                    The result from the mult-query
                """
                return_body = await self.stringendpoint.header_signed(self.multi_query)
                return json.loads(return_body)

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
            def __init__(self, api_endpoint, client, ssl_verify_disabled=False):
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
                self.client = client
                self.stringendpoint = _StringEndpoint(api_endpoint, client, ssl_verify_disabled)

            async def transaction(self, transaction_obj, deps=None, do_await=True):
                """Transact with list of python dicts that should get serialized to JSON,
                returns a transaction handle for polling FlureeDB if needed.

                Parameters
                ----------
                transaction_obj : list
                               Transaction list
                deps: dict
                    FlureeDb debs

                do_await: bool
                    Do we wait for the transaction to complete, or do we fire and forget?

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
                if tid[0] == '"':
                    tid = tid[1:-1]
                else:
                    tid = json.loads(tid)["id"]
                if not do_await:
                    return tid
                try_count = 0
                while True:
                    try_count += 1
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
            def __init__(self, client, ssl_verify_disabled=False):
                """Constructor

                Parameters
                ----------
                client: object
                        The wrapping _FlureeDbClient
                ssl_verify_disabled: bool
                        When using https, don't validata ssl certs.
                """
                self.stringendpoint = _StringEndpoint('ledger_stats', client, ssl_verify_disabled)

            async def __call__(self):
                """Send request to ledger-stats endpoint and retrieve result

                Returns
                -------
                dict
                    json decode result from the server.
                """
                return_body = await self.stringendpoint.empty_post_unsigned()
                return json.loads(return_body)

        class StringQueryEndpoint:
            """Endpoint for low level string querying (sql/sparql endpoints)"""
            def __init__(self, endpoint, client, ssl_verify_disabled=False):
                """Constructor

                Parameters
                ----------
                endpoint: string
                        Name of the endpoint
                client: object
                        The wrapping _FlureeDbClient
                ssl_verify_disabled: bool
                        When using https, don't validata ssl certs.
                """
                self.stringendpoint = _StringEndpoint(endpoint, client, ssl_verify_disabled)

            async def __call__(self, query_string):
                """Send request to ledger-stats endpoint and retrieve result

                Parameters
                ----------
                query : string
                        Query in the proper query language (sql or sparql depending on endpoint name)

                Returns
                -------
                dict
                    json decode result from the server.
                """
                return_body = await self.stringendpoint.header_signed(query_string, contenttype="text/plain")
                return json.loads(return_body)

        if api_endpoint not in self.known_endpoints:
            raise AttributeError("FlureeDB has no endpoint named " + api_endpoint)
        if api_endpoint not in self.implemented:
            raise NotImplementedError("No implementation yet for " + api_endpoint)
        if api_endpoint in ["command"]:
            if self.signer is None:
                raise FlureeKeyRequired("Command endpoint not supported in open-API mode. privkey required!")
            return CommandEndpoint(api_endpoint, self, self.ssl_verify_disabled)
        if api_endpoint in ["multi_query"]:
            return FlureeQlEndpointMulti(self, self.ssl_verify_disabled)
        if api_endpoint == 'ledger_stats':
            return LedgerStatsEndpoint(self, self.ssl_verify_disabled)
        if api_endpoint in ["sql", "sparql"]:
            return StringQueryEndpoint(api_endpoint, self, self.ssl_verify_disabled)
        return FlureeQlEndpoint(api_endpoint, self, self.ssl_verify_disabled)
