"""Fluree Domain-API Template processing

This library provides some glue code to make a rudimentary domain API with FlureeDB
queries and transactions. Right now it is a mostly separate API within aioflureedb.

For the future, a tighter integration is desired.
"""
import json
import os
from pyjsonata import jsonata


def _detemplate_object(kwargs, template):
    """Convert an object/dict type template to query or transaction chunk"""
    # start off with an empty return value dict
    rval = dict()
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
            # If the val is a list, detemplate that list.
            rval[key] = _detemplate_list(kwargs, val)
        elif isinstance(val, dict):
            # If the val is another dict, detemplate that dict
            rval[key] = _detemplate_object(kwargs, val)
        else:
            # Anything else is probably bool or numeric, copy as is.
            rval[key] = val
    return rval


def _detemplate_list(kwargs, template):
    """Convert a list type template to query or transaction chunk"""
    # start of with empty list as return value
    rval = list()
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
        self.template = template
        self.expander = expander

    def __call__(self, **kwargs):
        """Expand template with operation"""
        # detemplate operation
        operation = _detemplate_object(kwargs, self.template)
        # expand transaction
        self.expander.transaction.append(operation)


class _Expander:
    # pylint: disable=too-few-public-methods
    """Transaction Expander"""
    def __init__(self, transaction, name, collection):
        self.transaction = transaction
        self.name = name
        self.collection = collection

    def __call__(self):
        """Return the whole transaction as a python list"""
        return self.transaction

    def __getattr__(self, name):
        """Get a functor for adding named operators to the transaction, using a template again"""
        long_name = self.name + "/" + name
        if long_name in self.collection.transactions:
            operation = self.collection.templates[long_name]
            return _ExpanderFunction(operation, self)
        # Raise exception if not part of role sub-API
        raise AttributeError("No template " + name + " for expanding transaction " + self.name)


class _Transformer:
    # pylint: disable=too-few-public-methods
    """Query Transformer"""
    def __init__(self, query, xform):
        self.query = query
        self.xform = xform

    def __call__(self, query_result=None):
        """Get the query as dict, or transform the query result"""
        if query_result is None:
            # If no query_result is provided, we return the detemplated query itself.
            return self.query
        # Otherwise, we transform the query result as defined in the transformation jsonata file
        if self.xform is None:
            return query_result
        return json.loads(jsonata(self.xform, json.dumps(query_result)))


class _TemplateCollection:
    # pylint: disable=too-few-public-methods
    """The core template collection class used for role sub-APIs"""
    def __init__(self, transactions, queries, apimapdir):
        # Set with possible transactions
        self.transactions = set(transactions)
        # Set with possible templates
        self.valid = set(queries).union(self.transactions)
        # Set with the templates that actually have been used. Meant for coverage metrics.
        self.used = set()
        # The templates for the role
        self.templates = dict()
        # The transformation expressions for the query templates
        self.xform = dict()
        # Directory where all the templates, xform files and role definitions reside
        self.apimapdir = apimapdir
        # Initialize the templates dict.
        self._init_templates("transaction", transactions)
        self._init_templates("query", queries)

    def _init_templates(self, subdir, templates):
        if templates:
            templatedir = os.path.join(self.apimapdir, subdir)
            for template in templates:
                # The default path for a template
                filename = template + ".json"
                template_path = os.path.join(templatedir, filename)
                try:
                    # First try the default path
                    with open(template_path) as template_file:
                        self.templates[template] = json.load(template_file)
                except FileNotFoundError:
                    # If that fails, try the alternative subdir based path for transactions
                    alt_filename = template + "/default.json"
                    template_path = os.path.join(templatedir, alt_filename)
                    with open(template_path) as template_file:
                        self.templates[template] = json.load(template_file)
                # For queries, try to load the jsonata xform expression if it exists.
                filename2 = template + ".xform"
                xform_path = os.path.join(templatedir, filename2)
                try:
                    with open(xform_path) as xform_file:
                        self.xform[template] = xform_file.read()
                except FileNotFoundError:
                    pass

    def __getattr__(self, name):
        """Automatically map the role config to valid methods"""
        class Transaction:
            """Transaction functor"""
            def __init__(self, collection, name):
                self.name = name
                self.template = collection.templates[name]
                self.collection = collection

            def __call__(self, **kwargs):
                transaction = _detemplate_list(kwargs, self.template)
                return _Expander(transaction, self.name, self.collection)

        class Query:
            """Query functor"""
            def __init__(self, collection, name):
                self.name = name
                self.template = collection.templates[name]
                self.xform = None
                if name in collection.xform:
                    self.xform = collection.xform[name]

            def __call__(self, **kwargs):
                query = _detemplate_object(kwargs, self.template)
                return _Transformer(query, self.xform)

        # Return a query functor or transaction functor depending on config.
        if name in self.valid:
            self.used.add(name)
            if name in self.transactions:
                return Transaction(self, name)
            return Query(self, name)
        raise AttributeError("No " + name + " method for role")

    def coverage(self):
        """Return the percentage of domain API coverage so far"""
        return int(10000*float(len(self.used))/float(len(self.valid)))/100


class FlureeDomainAPI:
    # pylint: disable=too-few-public-methods
    """Highest level object for encapsulating full daomain API"""
    def __init__(self, apimapdir):
        self.apimapdir = apimapdir

    def get_api_by_role(self, role):
        """Get the (non-integrated) sub-API for a specific role"""
        rolesdir = os.path.join(self.apimapdir, "roles")
        filename = role + ".json"
        role_path = os.path.join(rolesdir, filename)
        with open(role_path) as role_file:
            role = json.load(role_file)
        return _TemplateCollection(role["transactions"], role["queries"], self.apimapdir)

    def get_async_api_by_role(self, role, database):
        """Get the (fully-integrated) sub-API for a specific role"""
        raise NotImplemented("Full template-based domain-API integration has not yet been implemented.")
