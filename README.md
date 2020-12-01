## aioflureedb

This library isn't fully production stable yet; Alpha.

### Install

To install the 0.1 release, run

```bash
python3 -m pip install aioflureedb
```

To install the dev snapshot instead (not recomended) run.
```bash
git clone https://github.com/pibara/aioflureedb.git
cd aioflureedb
python3 -m pip install .
```


### Testing the low level signing lib (dev snapshot only)
If you wish to run the test scripts, install the FlureeDB Javascript library:
This shouldn't be needed for normal operation.

```bash
npm install @fluree/crypto-utils
```

After that, you should be able to run:

```bash
./test-signing.sh
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

Other endpoints are curently not supported yet.

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


