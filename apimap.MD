# Making custom domain APIs for aioflureedb with templates and JSONata expressions

The aioflureedb API now has the possibility to plug in a templated domain-API. 
The definition of such an API is done in a **apimap** directory. At the root of 
the apimap directory we find three sub directories.

* query
* transaction
* roles

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

TODO

## The roles directory

TODO
