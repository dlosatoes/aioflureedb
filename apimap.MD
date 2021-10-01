# Making custom domain APIs for aioflureedb with templates and JSONata expressions

The aioflureedb API now has the possibility to plug in a templated domain-API. 
The definition of such an API is done in a **apimap** directory. At the root of 
the apimap directory we find three sub directories.

* query
* transaction
* roles

Next to these directories that are used by *aioflureedb*, there is a directory that will be used in (the upcomming version) of the [Fluree Schema Scenario Tool](https://github.com/pibara/fluree-schema-scenario-tool). Thede files are meant for integration testing the apimap and underlying fluree schema.

* fsst

Discussion of the last directory falls outside of the scope of this document.

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

Note that xform files are optional, if not pressent, the raw flureedb response will be returned (as python dict or list).

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

## The ffst directory

The ffst directory is meant to contain integration testing scenarios for testing the apimap and schema against API usage patterns. Discussion of this directory falls outside of the scope of this document.

# Future use of the apimap dir

The usage of the apimap by *aioflureedb* is one of three places the above templates and transformation expressions. It is planned that the *apimap* directory stucture will be used by the next version of the [Fluree Schema Scenario Tool](https://github.com/pibara/fluree-schema-scenario-tool) to aid in supporting CICD-pipeline integration testing of the schema with the domain API, and a minimal JavaScript utility library is planned for allowing the *apimap* directory (or a compiled single file JSON config derived from it) to be used from JavaScript too. Note that the later will be a utility library only without integration with an actual JavaScript FlureeDB library.

# Usage in aioflureedb with Python

For information on how the *apimap* directory is used in aioflureedb and how to use it in your own Python code for creating and using a convenient custom domain API in your code, keeping most (possibly all if all you use is the query and command endpoints) fluree queries out of your code, while opening up the door for in-pipeline integration testing with the [Fluree Schema Scenario Tool](https://github.com/pibara/fluree-schema-scenario-tool) (in the near future), check out the [section](https://github.com/pibara/aioflureedb/blob/master/API.md#your-own-domain-api) in the aioflureedb API doc.
