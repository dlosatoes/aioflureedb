"""Fluree Domain-API Template processing

This library provides some glue code to make a rudimentary domain API with FlureeDB
queries and transactions. Right now it is a mostly separate API within aioflureedb.

For the future, a tighter integration is desired.
"""
import json
import os
# pylint: disable=unspecified-encoding
AIOFLUREEDB_HAS_JSONATA = True
try:
    from jsonata import Context as JsonataContext
except ImportError:
    AIOFLUREEDB_HAS_JSONATA = False

    class JsonataContext:
        """Dummy Context for install witout jsonata"""
        def __call__(self, xform, json_data):
            """We don't want to fail on import, we only want to fail on usage

            Parameters
            ----------

            xform : str
                Transformation code
            json_data : str
                Source json
            Raises
            ------
            RuntimeError
                Raised always because of missing jsonata library
            """
            raise RuntimeError("Domain-API method uses a jsonata transformation-file while jsonata module is not available.")


def _detemplate_cell(value, template):
    """Convert a "$" containing template chunk

    Parameters
    ----------
    value : str
        The value to use for "$"
    template: any
        Template or part of a template that needs expanding.

    Returns
    -------
    any
        returns a detemplated version of the input template.
    """
    if isinstance(template, str):
        if template == "$":
            return value
        return template
    if isinstance(template, list):
        rval = []
        for entry in template:
            rval.append(_detemplate_cell(value, entry))
        return rval
    if isinstance(template, dict):
        rval2 = {}
        for key in template.keys():
            rval2[key] = _detemplate_cell(value, template[key])
        return rval2
    return template


def _detemplate_object(kwargs, template):
    """Convert an object/dict type template to query or transaction chunk

    Parameters
    ----------
    kwargs : dict
        Key/value pairs for patching template with.
    template : dict
        Template or part of a template that needs expanding.

    Returns
    -------
    dict
        result from patching the template with kwargs.

    Raises
    ------
    ValueError
        Raised if missing kwarg key
    """
    # pylint: disable=too-many-branches
    # start off with an empty return value dict
    rval = {}
    # Itterate over all key/value pairs in the template
    for key, val in template.items():
        if isinstance(val, str):
            # If the value is a string, first check if it's an erasure key
            if key[:2] == "::" and val[:2] == "::":
                # Only if the variable named in the val is in the kwargs, put the patched
                # key/value in the result.
                optional_name = key[2:]
                optional_replace = val[2:]
                if optional_replace in kwargs.keys():
                    rval[optional_name] = kwargs[optional_replace]
            elif val[:2] == "::":
                # If only the value is a template var, replace it or throw an exception
                # if kwargs is missing an argument.
                optional_replace = val[2:]
                if optional_replace in kwargs.keys():
                    rval[key] = kwargs[optional_replace]
                else:
                    raise ValueError("Missing argument '" + optional_replace + "'")
            else:
                # Other strings are just copied as is.
                rval[key] = val
        elif isinstance(val, list):
            # If the val is a list, first check if it's an erasure array
            if key[:4] == "::[]":
                if ":" in key[4:]:
                    optional_name, varname = key[4:].split(":")
                else:
                    optional_name = key[4:]
                    varname = optional_name

                if varname in kwargs.keys() and isinstance(kwargs[varname], list):
                    possible_val = []
                    for value in kwargs[varname]:
                        possible_val.append(_detemplate_cell(value, val))
                if possible_val:
                    rval[optional_name] = possible_val
            else:
                rval[key] = _detemplate_list(kwargs, val)
        elif isinstance(val, dict):
            # If the val is another dict, detemplate that dict
            rval[key] = _detemplate_object(kwargs, val)
        else:
            # Anything else is probably bool or numeric, copy as is.
            rval[key] = val
    return rval


def _detemplate_list(kwargs, template):
    """Convert a list type template to query or transaction chunk

    Parameters
    ----------
    kwargs : dict
        Key/value pairs for patching template with.
    template : list
        Template or part of a template that needs expanding.

    Returns
    -------
    list
        result from patching the template with kwargs.

    Raises
    ------
    ValueError
        Raised if missing kwarg key
    """
    # start of with empty list as return value
    rval = []
    # Ifferate all source items from the list
    for operation in template:
        if isinstance(operation, str):
            if operation[:2] == "::":
                # Try to replace :: template vars
                optional_replace = operation[2:]
                if optional_replace in kwargs.keys():
                    rval.append(kwargs[optional_replace])
                else:
                    # If the named variable isn't in the template raise an exception.
                    raise ValueError("Missing argument '" + optional_replace + "'")
            else:
                rval.append(operation)
        elif isinstance(operation, dict):
            # If the val is a dict, detemplate that dict
            rval.append(_detemplate_object(kwargs, operation))
        elif isinstance(operation, list):
            # If the val is another list, detemplate that list.
            rval.append(_detemplate_list(kwargs, operation))
        else:
            # Anything else is probably bool or numeric, copy as is.
            rval.append(operation)
    return rval


class _ExpanderFunction:
    # pylint: disable=too-few-public-methods
    """Functor for expanding transactions with an operation template."""
    def __init__(self, template, expander):
        """Constructor

        Parameters
        ----------
        template : dict
            Template for operation within a transaction
        expander : _Expander
            Parent transaction _Expander object
        """
        self.template = template
        self.expander = expander

    def __call__(self, *args, **kwargs):
        """Expand template with operation

        Parameters
        ----------

        args : list
            Positional arguments, these will be ignored!

        kwargs : dict
            key value dict with arguments
        """
        # detemplate operation
        operation = _detemplate_object(kwargs, self.template)
        # expand transaction
        self.expander.transaction.append(operation)


class _Expander:
    # pylint: disable=too-few-public-methods
    """Transaction Expander"""
    def __init__(self, transaction, name, collection):
        """Constructor

        Parameters
        ----------
        transaction : list
            Transaction list template
        name : string
            Name of the transaction
        collection: _TemplateCollection
            Parent Template collection
        """
        self.transaction = transaction
        self.name = name
        self.collection = collection

    def __call__(self):
        """Return the whole transaction as a python list

        Returns
        -------
        list
            The transaction
        """
        return self.transaction

    def __getattr__(self, name):
        """Get a functor for adding named operators to the transaction, using a template again

        Parameters
        ----------
        name : string
            Name of the transaction sub-operation method

        Returns
        -------
        _ExpanderFunction
            Object that handles the actual call of the simulated method.

        Raises
        ------
        AttributeError
            Transaction doesn't have any such named sub operation template.
        """
        long_name = self.name + "/" + name
        if long_name in self.collection.transactions:
            operation = self.collection.templates[long_name]
            return _ExpanderFunction(operation, self)
        # Raise exception if not part of role sub-API
        raise AttributeError("No template " + name + " for expanding transaction " + self.name)


class _AsyncExpander:
    # pylint: disable=too-few-public-methods
    """Transaction Expander"""
    def __init__(self, transaction, name, collection, database):
        """Constructor

        Parameters
        ----------
        transaction : list
            Transaction template
        name : string
            Name of the transaction
        collection : _TemplateCollection
            Parent Template Collection object
        database : _FlureeDbClient
            Active FlureeDB client session
        """
        self.transaction = transaction
        self.name = name
        self.collection = collection
        self.database = database

    async def __call__(self):
        """Execute transaction

        Returns
        -------
        dict
            Transaction status object
        """
        return await self.database.command.transaction(self.transaction)

    def __getattr__(self, name):
        """Get a functor for adding named operators to the transaction, using a template again

        Parameters
        ----------
        name : string
            Name of the simulated method.

        Returns
        -------
        _ExpanderFunction
            Function for expanding the current transaction before executing.

        Raises
        ------
        AttributeError
            Thrown if there is no template defined fore this method.
        """
        long_name = self.name + "/" + name
        if long_name in self.collection.transactions:
            operation = self.collection.templates[long_name]
            return _ExpanderFunction(operation, self)
        # Raise exception if not part of role sub-API
        raise AttributeError("No template " + name + " for expanding transaction " + self.name)


class _Transformer:
    # pylint: disable=too-few-public-methods
    """Query Transformer"""
    def __init__(self, query, xform, jsonatacontext):
        """Constructor

        Parameters
        ----------
        query : dict
            Rendered result from query template
        xform : string
            JSONata transformation expression
        """
        self.query = query
        self.xform = xform
        self.jsonatacontext = jsonatacontext

    def __call__(self, query_result=None):
        """Get the query as dict, or transform the query result

        Parameters
        ----------
        query_result : dict or list
            Result from invoking this query, optional

        Returns
        -------
        list or dict
            Either the query (if no arguments) or the JSONata transformed query result.
        """
        if query_result is None:
            # If no query_result is provided, we return the detemplated query itself.
            return self.query
        # Otherwise, we transform the query result as defined in the transformation jsonata file
        if self.xform is None:
            return query_result
        rval = self.jsonatacontext(self.xform, query_result)
        if rval == 'undefined':
            return None
        return json.loads(rval)


class _TemplateCollection:
    # pylint: disable=too-few-public-methods, too-many-instance-attributes
    """The core template collection class used for role sub-APIs"""
    def __init__(self, transactions, queries, multi, apimapdir, apimap, database, jsonatacontext):
        """Constructor

        Parameters
        ----------
        transactions : list
            list of transaction template names
        queries : list
            list of query template names
        multi: list
            list of multi-query template names
        apimapdir: string
            path where the api map files are
        apimap: dict
            if the api map is a json file, this argument has it's content
        database : _FlureeDbClient
            Optional fluree database session
        """
        # pylint: disable=too-many-arguments
        # Set with possible transactions
        self.transactions = set(transactions)
        # Set with multi-queries
        self.multi = set(multi)
        # Set with possible templates
        self.valid = set(queries).union(self.transactions).union(self.multi)
        # Set with the templates that actually have been used. Meant for coverage metrics.
        self.used = set()
        # The templates for the role
        self.templates = {}
        # The transformation expressions for the query templates
        self.xform = {}
        # Directory where all the templates, xform files and role definitions reside
        self.apimapdir = apimapdir
        # And if used the deserialized JSON
        self.apimap = apimap
        # database or None
        self.database = database
        # Initialize the templates dict.
        self._init_templates("transaction", transactions)
        self._init_templates("query", queries)
        self._init_templates("multi", multi)
        self.jsonatacontext = jsonatacontext

    def _init_templates(self, subdir, templates):
        """Initialize the templates dicts for this role

        Parameters
        ----------
        subdir : string
            Either transactions or queries
        templates : list
            Names of the templates of this type

        Raises
        ------
        RuntimeError
            Raised if jsonata used but not available on platform

        """
        # pylint: disable=too-many-branches
        if templates:
            if self.apimap is None:
                templatedir = os.path.join(self.apimapdir, subdir)
            else:
                templatedir = None
            ignore_xform = os.environ.get('AIOFLUREEDB_IGNORE_XFORM') is not None
            deep_fail = os.environ.get('AIOFLUREEDB_XFORM_DEEP_FAIL') is not None
            for template in templates:
                if templatedir is not None:
                    # The default path for a template
                    filename = template + ".json"
                    template_path = os.path.join(templatedir, filename)
                    try:
                        # First try the default path
                        with open(template_path) as template_file:
                            self.templates[template] = json.load(template_file)
                    except FileNotFoundError:
                        # If that fails, try the alternative subdir based path for transactions
                        alt_filename = os.path.join(template, "default.json")
                        template_path = os.path.join(templatedir, alt_filename)
                        with open(template_path) as template_file:
                            self.templates[template] = json.load(template_file)
                    # For queries, try to load the jsonata xform expression if it exists.
                    filename2 = template + ".xform"
                    xform_path = os.path.join(templatedir, filename2)
                    try:
                        with open(xform_path) as xform_file:
                            if AIOFLUREEDB_HAS_JSONATA or deep_fail:
                                self.xform[template] = xform_file.read()
                            elif not ignore_xform:
                                raise RuntimeError(
                                    "API-Map uses jsonata transformation files while pyjsonata module is not available."
                                )
                    except FileNotFoundError:
                        pass
                else:
                    if subdir in self.apimap and template in self.apimap[subdir]:
                        self.templates[template] = self.apimap[subdir][template]
                    else:
                        print("1", subdir)
                        print("2", self.apimap.keys())
                        print("3", template)
                        print("4", self.apimap[subdir].keys())
                        raise RuntimeError("Can't locate template in api map file:" + subdir + "::" + template)
                    if subdir in ["query", "multi"]:
                        if "xform" in self.apimap and template in self.apimap["xform"]:
                            if AIOFLUREEDB_HAS_JSONATA or deep_fail:
                                self.xform[template] = self.apimap["xform"][template]
                            elif not ignore_xform and self.apimap["xform"][template] != "$":
                                raise RuntimeError(
                                    "API-Map uses jsonata transformation files while pyjsonata module is not available."
                                )

    def __getattr__(self, name):
        """Automatically map the role config to valid methods

        Parameters
        ----------
        name : string
            Role API method name

        Returns
        -------
        callable
            Depending if name maps to a query or transaction and if database is set

        Raises
        ------
        AttributeError
            Raised if name not known.

        """

        class Transaction:
            """Transaction functor"""
            def __init__(self, collection, name, database):
                """Constructor

                Parameters
                ----------
                collection : _TemplateCollection
                    Parent Template Collection
                name : string
                    Name of the transaction
                database : _FlureeDbClient
                    Active database session
                """
                self.name = name
                self.template = collection.templates[name]
                self.collection = collection
                self.database = database

            def __call__(self, *args, **kwargs):
                """Invocation function object

                Parameters
                ----------

                args : list
                    Positional arguments, these will be ignored!

                kwargs : dict
                    Keyword arguments

                Returns
                -------
                _Expander or _AsyncExpander
                    Depends on if database is supplied
                """
                transaction = _detemplate_list(kwargs, self.template)
                if self.database is None:
                    return _Expander(transaction, self.name, self.collection)
                return _AsyncExpander(transaction, self.name, self.collection, self.database)

        class Query:
            """Query functor"""
            def __init__(self, collection, name, jsonatacontext):
                """Constructor

                Parameters
                ----------
                collection : aioflureedb.domain_api._TemplateCollection
                             Parent Template Collection
                name : string
                       Name of the query
                """
                self.name = name
                self.template = collection.templates[name]
                self.xform = None
                if name in collection.xform:
                    self.xform = collection.xform[name]
                self.jsonatacontext = jsonatacontext

            def __call__(self, *args, **kwargs):
                """Invoke

                Parameters
                ----------

                args : list
                    Positional arguments, these will be ignored!

                kwargs : dict
                    keyword arguments

                Returns
                -------
                _Transformer
                    Query object that can also transform query results.
                """
                query = _detemplate_object(kwargs, self.template)
                return _Transformer(query, self.xform, self.jsonatacontext)

        class AsyncQuery:
            """Query functor"""
            def __init__(self, query, database):
                """Constructor

                Parameters
                ----------
                query : dict
                    The query dict as produced by template.
                database : _FlureeDbClient
                    Active database session
                """
                self.query = query
                self.database = database

            async def __call__(self, *args, **kwargs):
                """Async Invoke

                Parameters
                ----------

                args : list
                    Positional arguments, these will be ignored!

                kwargs : dict
                    Keyword arguments

                Returns
                -------
                dict or list
                    Query result from flureedb, possibly transformed
                """
                ll_query = self.query(**kwargs)
                ll_result = await self.database.flureeql.query.raw(ll_query())
                return ll_query(ll_result)

        class AsyncMultiQuery:
            """Query functor"""
            def __init__(self, query, database):
                """Constructor

                Parameters
                ----------
                query : dict
                    The query dict as produced by template.
                database : _FlureeDbClient
                    Active database session
                """
                self.query = query
                self.database = database

            async def __call__(self, *args, **kwargs):
                """Async Invoke

                Parameters
                ----------

                args : list
                    Positional arguments, these will be ignored!

                kwargs : dict
                    Keyword arguments

                Returns
                -------
                dict or list
                    Query result from flureedb, possibly transformed
                """
                ll_query = self.query(**kwargs)
                ll_result = await self.database.multi_query(ll_query()).query()
                return ll_query(ll_result)

        # Return a query functor or transaction functor depending on config.
        if name in self.valid:
            self.used.add(name)
            if name in self.transactions:
                return Transaction(self, name, self.database)
            if name in self.multi:
                if self.database is None:
                    return Query(self, name, self.jsonatacontext)
                return AsyncMultiQuery(Query(self, name, self.jsonatacontext), self.database)
            if self.database is None:
                return Query(self, name, self.jsonatacontext)
            return AsyncQuery(Query(self, name, self.jsonatacontext), self.database)
        raise AttributeError("No " + name + " method for role")

    def coverage(self):
        """Return the percentage of domain API coverage so far

        Returns
        -------
        float
            The percentage of templates covered so far
        """
        return int(10000*float(len(self.used))/float(len(self.valid)))/100


class FlureeDomainAPI:
    # pylint: disable=too-few-public-methods
    """Highest level object for encapsulating full domain API"""
    def __init__(self, apimapdir, database=None, bigint_patch=False):
        """Constructor

        Parameters
        ----------
        apimapdir : string
            Directory where apimap resides
        database: _FlureeDbClient
            Active database session
        """
        if os.path.isdir(apimapdir):
            self.apimapdir = apimapdir
            self.apimap = None
        elif os.path.splitext(apimapdir)[1].lower() == ".json":
            self.apimapdir = None
            with open(apimapdir) as mapfil:
                self.apimap = json.load(mapfil)
        self.database = database
        self.jsonatacontext = JsonataContext(bigint_patch=bigint_patch)

    def get_api_by_role(self, role):
        """Get the sub-API that is available to a given role

        Parameters
        ----------
        role : string
            Name of the role

        Returns
        -------
        _TemplateCollection
            Template collection for the given role
        """
        if self.apimap is None:
            rolesdir = os.path.join(self.apimapdir, "roles")
            filename = role + ".json"
            role_path = os.path.join(rolesdir, filename)
            with open(role_path) as role_file:
                myrole = json.load(role_file)
        else:
            myrole = self.apimap["roles"][role]
        if "transactions" in myrole:
            transactions = myrole["transactions"]
        else:
            transactions = []
        if "queries" in myrole:
            queries = myrole["queries"]
        else:
            queries = []
        if "multi" in myrole:
            multi = myrole["multi"]
        else:
            multi = []
        return _TemplateCollection(transactions, queries, multi,
                                   self.apimapdir, self.apimap, self.database,
                                   self.jsonatacontext)
