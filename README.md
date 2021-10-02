## aioflureedb

This library isn't fully production stable yet; Most of the code should be considered of late beta quality and stability. 
This with the exception of the domain-API code that at moment of writing should be considered alpha.

### Install

To install run:

```bash
python3 -m pip install aioflureedb
```

### API usage

* [High level API](API.md)
* [Low level signing API](SIGNING.md)


## Work in progress

The aioflureedb library is currently functioning for the API endpoints that it implements.
These are:

* health  (fluree client)
* dbs     (fluree client)
* query   (database client)
* block   (database client)
* command (database client)
* ledger_stats (database client)
* list_snapshots (database client)
* snapshot (database client)

Other endpoints (export, multi_query, history, transact, graphql, sparql, reindex, hide, gen_flakes, query_with, test_transact_with, block_range_with, storage, pw) are curently not supported yet.

### Help prioritize!

Some of the other endpoint may be quite easy to implement, but there are quite a lot of them to choose from and prioritize.

If you feel the API is lacking endpoint support that you need for your project, please create [a project issue](https://github.com/pibara/aioflureedb/issues) to help us prioritize development.

### Help with code

**Pull request** with API-endpoint code are very much welcome.

If you do (plan to) create a pull request, please:

* Fork the project.
* Create an issue, mentioning you are working on it.
* Run pylint on your patched code using the supplied .pylintrc
* Run pycodestyle on your patched code using the *--max-line-length=128* option
* Create the pull request


