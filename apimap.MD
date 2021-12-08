# Making custom domain APIs for aioflureedb with templates and JSONata expressions

The aioflureedb API now has the possibility to plug in a templated domain-API. 
The definition of such an API is done in a **apimap** directory. At the root of 
the apimap directory we find three sub directories.

* query
* transaction
* roles

Next to these directories that are used by *aioflureedb*, there is a directory that will be used in (the upcomming version) of the [Fluree Schema Scenario Tool](https://github.com/pibara/fluree-schema-scenario-tool). Thede files are meant for integration testing the apimap and underlying fluree schema.

* fsst

A directory used by the fsst tool to integrate domain-API tests into the **fsst test* and **fsst dockertest** commands. This directory for a great part holds the same structure as the top-level directory. It has a *query*, *transaction* and a *roles* subdir that work exactly the same as the top level directory with just a minor difference: The roles are testing roles. There is a *root* role and a *test* role and these roles are used for defining a helper API that can be used to communicate with FlureeDB from within a test. 

The *fsst* directory has one extra sub directory that doesn't exist at the top level: The *tests* directory. 
The test directory contains test modules, written in Python, for testing the API for a specific role. Note that version 0.3 or higher of the fsst tool is required to use these tests.

## The query directory

In the *query* directory we should have a collection of query templates. A query template is basically a JSON file
containing what is almost a complete FlureeQL query. In fact, a query template can simply be a valid FlureeQL query.
The name of the file will end up becoming the name of a python method, so it is suggested to use python method naming conventions for the files.

A straight flureeql template could look something like this

```json
{
  "select": [{"?user": ["username","doc",{"auth": ["id"]}]}],
  "where": [
        ["?role", "_role/id", "demo_role"],
        ["?auth", "_auth/roles", "?role"],
        ["?user","_user/auth","?auth"]
  ]
}
```

If this fill is named *get_demo_users.json*, this would basically be enough for aioflureedb to expose a method *get_demo_users".  The result from fluree might however not be the ideal result for a python API. So to overcome this we create a second file, with the same name but an other file extention, 'xform'. This second file will be used to post-process the query result before returning it to the user. For such transformations, we use [JSONata expressions](https://jsonata.org/). In this case, our JSONata expression in the file *get_demo_users.xform* could look something like this:

```
$[].{"name": username, "email": doc, "pubkey": auth[0].id}
```

Note that xform files are optional, and are not supported on all platforms. If not pressent, the raw flureedb response will be returned (as python dict or list).

### jsonata and platform dependencies

The xform files described above depend on the [pyjsonata[(https://pypi.org/project/pyjsonata/) module that unfortunately doesnt work on MS-Windows. On Linux and Docker pyjsonata works just fine. It is unknown (currently assumed it doesn't) if pyjsonata works on other platforms.  If this assumption is wrong, install pyjsonata on your system before installing aioflureedb and please notify the authors of aioflureedb they should add pyjsonata on your platform.

If you install aioflureedb on a system without pyjsonata support, by default aioflureedb will throw a *RuntimeError* when loading an api-map directory. This behaviour can be changed by setting one of two environment variables:

* AIOFLUREEDB_IGNORE_XFORM : If this enviropnment variable is set to any value, the existance of xform files will get ignored as if the files werent there.

* AIOFLUREEDB_XFORM_DEEP_FAIL : If this enviropnment variable is set to any value, no exception will be thrown on api-map directory load. Instead a *RuntimeError* will be thrown when a domain API method is called for what an xform file has been defined.

### Template argument anotation

A template isn't much of a template witout template arguments. Right now "*demo_role*" is hard coded into the query. We could turn the role into a template parameter *role* like this:

```json
{
  "select": [{"?user": ["username","doc",{"auth": ["id"]}]}],
  "where": [
        ["?role", "_role/id", "::role"],
        ["?auth", "_auth/roles", "?role"],
        ["?user","_user/auth","?auth"]
  ]
}
```

To be replaced template arguments are designated with the double colon notation above.

## The transaction directory

The *transaction* directory, like the *query* directory contains template JSON files. Other than in the *query* directory though, the *transaction* directory can also contain sub directories. Sub-directories are meant for dynamically composed transactions. Please note that in the current implementation dynamically composition transaction sub directories might not work in an MS-windows environment. 

A basic transaction template, just like a query can simply be just a flureeql transaction.

```json
[
    {
        "_id":"_role",
        "id":"demo_role",
        "doc": "The Demo Role, just an other root",
        "rules": [["_rule/id", "root"]]
    }
]
```

And like for *query* templates, it can contain template parameters that work exactly the same.

```json
[
    {
        "_id":"_user$new_user",
        "username":"::full_name",
        "doc": "::email",
        "auth": ["_auth$new_user"]
    },
    {
        "_id": "_auth$new_user",
        "id": "::pubkey",
        "doc": "::email",
        "roles": [["_role/id", "demo_role"]]
    }
]
```
If the above template is named *create_demo_user* and is inside of the file *create_demo_user.json*, it is a non-extendable transaction. If instead we want to be able to create a transaction with optional aditional operations appended to it, we need to move the template file to *create_demo_user/default.json* instead.

In doing so, we create the possibility to create *operation templates* in the newly created template directory. Let's say our schema has a collection *resource* and a collection *resource_access*, and we want to be able to grant access to the resources in a single transaction. We could create an operation template in the file *create_demo_user/add_resource.json*

```json
{
   "_id": "resource_access",
   "resource_id": ["resource/name", "::resource"],
   "user_id": "_user$new_user"
}
```

One last thing to discuss are non-mandatory template parameters. If in the *create_demo_user* we want the email template parameter to be optional, for fluree transactions we will simply want the two *doc* predicates in our transaction to get erased from the result transaction. To indicate '*erase key/val if template parameter is missing*' we use the double collon again, but this time in the key as well.

```json
[
    {
        "_id":"_user$new_user",
        "username":"::full_name",
        "::doc": "::email",
        "auth": ["_auth$new_user"]
    },
    {
        "_id": "_auth$new_user",
        "id": "::pubkey",
        "::doc": "::email",
        "roles": [["_role/id", "demo_role"]]
    }
]
```

## The roles directory

The *roles* directory is there to map the available *query* and *transaction* templates to roles withinf FlureeDB. The roles are assumed to have been created in the fluree schema deployment. The directory contains JSON files, one file per role, that each contain two lists. One list listing the queries this role is expected to use. The other containing transaction the the role is expected to do. 

```json
{
    "transactions": [
        "create_demo_user_role",
        "create_demo_user"
    ],
    "queries": [
        "get_demo_users"
    ]
}
```

## Domain-API unit-tests in the ffst/tests directory

Files in the ffst/tests directory are Python files that define a testing module for a domain-API for a specific role. By convention the name of the file should be the name of the role, but if one role uses more than one testing module, other names are allowed also.

A module defines exactly one class named DomainApiTest. The definition should start like:

```python
class DomainApiTest:
    """Domain-API test class"""
    # pylint: disable=too-few-public-methods
    def __init__(self, test_env):
        """Constructor"""
        self.env = test_env
```

For every role-method the domain-API defines, one or more tests can be defined. These tests will run as the user defined in the fsst stage or step directory, and is supplied with two domain API objects. One with the actual domain API for the role, and an other one for the fsst helper API defined in the apimap. Naming of these test starts with **run_test_** followed by the name of the API mapping that is to be tested. If there is more than one test for a single API mapping, the name for all but the first test should be extended with an underscore and a sequence number

```python
    async def run_test_delete_user_by_auth(self, domain_api, test_api):
        return None

    async def run_test_delete_user_by_auth_1(self, domain_api, test_api):
        return None

    async def run_test_delete_user_by_auth_2(self, domain_api, test_api):
        return None
```

The return value of a test should be a boolean or a tuple of a boolean and a string

```
    return False, "Newly created user not found"
```

If a test is under construction, it is OK to return None instead. Please note None won''t be counted as a positive or negative and won''t count towards the coverage metric.

Prior to calling the API tests, there are two initializer methods that you can define if needed. The methods **prepare_root** and **prepare_user**. Inside of these methods transactions cn be done to prepare for the actual tests. The prepare_user method will be infoked prior to any test as the user/auth as what the tests will also be running.

```
async def prepare_user(self, domain_api, test_api):
        """Any prepare stuff we need to do as non-root test user"""
        ... # do some stuff here
        return True

```

It is also very much possible or even likely that the user/auth that runs the tests won't have sufficient rights to prepare the database for running the test. For this reason, prior to running prepare_user, the prepare_root method will be called if it exists, with two API maps running with root credentials

```python
async def prepare_root(self, domain_api, test_api):
        """Any prepare stuff we need to do as root"""
        await test_api.create_demo_role()()
        return True
```

When all tests are done running, there is a possibility to define one or two cleanup methods. Curently the use for cleanup is rather limited.

```python
    async def cleanup_root(self, domain_api, test_api):
        """Any cleanup we need to do as root"""
        return True

    async def cleanup_user(self, domain_api, test_api):
        """Any cleanup we need to do as non-root user"""
        return True
```

# Usage in aioflureedb with Python

For information on how the *apimap* directory is used in aioflureedb and how to use it in your own Python code for creating and using a convenient custom domain API in your code, keeping most (possibly all if all you use is the query and command endpoints) fluree queries out of your code, while opening up the door for in-pipeline integration testing with the [Fluree Schema Scenario Tool](https://github.com/pibara/fluree-schema-scenario-tool) (in the near future), check out the [section](https://github.com/pibara/aioflureedb/blob/master/API.md#your-own-domain-api) in the aioflureedb API doc.
