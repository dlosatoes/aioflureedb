# aioflureedb high-level API

This document is a quick walkthrough of the currently implemented part of the aioflureedb python API.

### Boilerplate
To use the library, first some asyncio boilerplate.

```python
import asyncio
import aioflureedb

async def fluree_main(privkey, addr):
    ...

PKEY = "3f9813dced3eebfd207c2a1f546d46f721731daf6155e1c9062d1f183bdb7159"
ADDRESS = "TfA9h2ub9zyosXU2ARFiEm68w21Y72tAYC5"
LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(fluree_main(PKEY, ADDRESS))

```
### Instantiating a client

The first thing that is needed in the async main is the creation of a FlureeClient. The default client uses the localhost FlureeDB on port 8080.

```python
async def fluree_main(privkey, addr):
   async with  aioflureedb.FlureeClient() as flureeclient:
      ...
```

In the 0.1 version of the API, all supported signed operations are on an existing database, so the below isn't usefull yet, but for the importance of a stable API, you can provide a private key and key id. These will be used when non-existing-db related signed operations get implemented (new\_db, delete\_db, add\_server, remove\_server).
```python
async def fluree_main(masterkey=privkey, addr):
    async with  aioflureedb.FlureeClient(masterkey=privkey, auth_addressaddr) as flureeclient:
       ...
```

There are some optional arguments to the constructor of the FlureeClient.
```python
async def fluree_main(privkey, addr):
    async with aioflureedb.FlureeClient(masterkey=privkey,
                                        auth_address=addr,
                                        host="fluree.demo.com",
                                        port=443,
                                        https=True,
                                        ssl_verify=False,
                                        sig_validity=600,
                                        sig_fuel=4321) as flureeclient:
       ...
```

For debugging purposes, the *dryrun* option will print the queries and transactions instead of posting them to the fluree server.
```python
async def fluree_main(privkey, addr):
    async with aioflureedb.FlureeClient(privkey, addr, dryrun=True) as flureeclient:
       ...
```

### Making sure FlureeDB is ready
The *health* endpoint has a convenience method *ready* that will run forever untill the database is ready.

```python
    ...
    await flureeclient.health.ready()
    ...
```
It is also possible to poll the health endpoint
```python
    ..
    ready = False
    try:
        fluree_health = await flureeclient.health()
        ready = fluree_health["ready"]
    except aioflureedb.FlureeHttpError:
        pass
    except aiohttp.client_exceptions.ClientConnectorError:
        pass
```


### Networks and databases

Once we have a FlureeClient, we can use it to itterate over the avilable networks and databases, and do what we need to do with each database.
```python
    async for network in flureeclient:
        for db in network:
            database = db(privkey, addr)
            ...
```

If needed, network and database itterator can be converted to strings

```python
    async for network in flureeclient:
        for db in network:
            netname = str(network)
            dbname = str(db)
            ...
```

Or if we already know the database we need, we can use square bracket notation.
```python
    try:
        network = await flureeclient["dev"]
        db = network["main"]
        ...
    except KeyError as exp:
        print("OOPS:", exp)
```

By default, the signatures for a single database will use the parameters of the FlureeClient. We can overrule two of them though.
```python
async def fluree_main(privkey, addr):
    ...
    async with db(privkey,
                  addr,
                  sig_validity=600,
                  sig_fuel=4321) as database:
        ...
    ...
```

It is important to note that the signing key used for things like database creation and that used for transacting with or querying the database most likely will not be the same.
```python
async def fluree_main(privkey1, addr1, privkey2, addr2):
    async with aioflureedb.FlureeClient(privkey1, addr1) as flureeclient:
        network = await flureeclient["dev"]
        db = network["main"]
    async with db(privkey2, addr2) as database:
       ...
```

Or for convenience:

```python
async def fluree_main(privkey1, addr1, privkey2, addr2):
    async with aioflureedb.FlureeClient(privkey1, addr1) as flureeclient:
        db = await flureeclient["dev/main"]
    async with db(privkey2, addr2) as database:
        ...
```

In case of an open API fluree host, no signing keys are needed.
```python
async def fluree_main():
    async with aioflureedb.FlureeClient() as flureeclient:
        network = await flureeclient["dev"]
        db = network["main"]
   with  db() as database:
    ...
```

### Queries

Once we have our database object, we can use it to access the query and the command API endpoints. Queries are done like this:
```python
   ...
   result = await database.query.query(
        select=["*"],
        ffrom="_user"
   )
   ...
```
Please note the use of the word **ffrom** instead of **from**. This is not a typo but a nesesity flowing from the fact that **from** is a reserved word in Python. Alternatively to the keyword query API, it is possible to use an all in one query object instead:

```python
   ...
   result = await database.query.query.raw({"select": ["*"]. "from": "_user"})
   ...
```

There is an alias *flureeql* for the query endpoint for eastetic reasons.

```python
   ...
   result = await database.flureeql.query(
        select=["*"],
        ffrom="_user"
   )
   ...
```

#### Block endpoint
Here is an example of a query on the block endpoint:
```python
   ...
   result = await database.block.query(block=[1,2])
   ...
```

### Transactions

Transactions use the command API endpoint, or actually a combination of the command API endpoint and the query API endpoint. Note that the Python API is subdivided by Fluree endpoint. An example of a transaction for adding a randomly named user.

```python
   ...
   randomuser = "user-" + str(int(time.time()) % 10000)
   try:
       transaction = await database.command.transaction([{"_id":"_user","username": randomuser}])
       ...
   except aioflureedb.FlureeTransactionFailure as exp:
       print("OOPS:", exp)

```

Optionally provide the transaction with deps (see Fluree documentation for usage)
```python
   ...
   randomuser = "user-" + str(int(time.time()) % 10000)
   transaction = await database.command.transaction([{"_id":"_user","username": randomuser}], deps=...)

```
By default an await on a transaction won't return untill the transaction either failed or succeeded. It is possible to split up
 the functionality of submitting a transaction and *polling* for readyness by setting do\_await explicitly to False.
```python
   ...
   randomuser = "user-" + str(int(time.time()) % 10000)
   tid = await database.command.transaction([{"_id":"_user","username": randomuser}], do_await=False)
   ..
       transaction = await database.query.query(
            select=["*"],
            ffrom=["_tx/id", tid]
       )
       if transaction:
          ...
   ...
```

### new-keys
If the client needs a new signing key with public key and key id, the **new\_keys** method lets you fetch them from flureedb

```python
  ...
  newkeys = await flureeclient.new_keys()
  signing_key = newkeys["private"]
  public_key = newkeys["public"]
  key_id = newkeys["account-id"]
  ..
```

### new-db

```python

   try:
       await flureeclient.new_db(db_id="dev/test6")
   except aioflureedb.FlureeHttpError as exp:
       print("Oops, problem creating database:", exp)

```

If you want to access a database directly after creating it, please note that when await returns, the database might not exist yet and queries and transactions could fail. The database object has a convenience awaitable *ready* method to address this problem.

```python
    ..
    async with  FlureeClient(privkey, addr, port=8090) as flureeclient:
        await flureeclient.health.ready()
        await flureeclient.new_db(db_id=semi_unique_db)
        db = await flureeclient[semi_unique_db]
    async with db(privkey, addr) as database:
        await database.ready()
    ..
```

### More comming up.

The above is the API for the 0.1 release of aioflureedb. Many API endpoints arent supported yet and none of them has currently been prioritized. Please [submit an issue](https://github.com/pibara/aioflureedb/issues) to help with prioritazion. Pull requests are also very much welcomed. Please make sure your patches don't brake either *pylint* with the [provided config](https://github.com/pibara/aioflureedb/blob/master/.pylintrc) or *pycodestyle* using the *--max-line-length=128* option.

#### TODO Utils

* Make use of the key-id in the API optional.

If you feel this requires priority, please [submit an issue](https://github.com/pibara/aioflureedb/issues), or fork and submit a pull request.

#### Todo Fluree client endpoint support

* delete\_db       : currently broken
* add\_server      : untested
* remove\_server   : untested

If you feel any of these requires priority, please [submit an issue](https://github.com/pibara/aioflureedb/issues) or fork and submit a pull request.

#### Todo Database client endpoint support

* snapshot
* list\_snapshots
* export
* multi\_query
* block
* history
* transact
* graphql
* sparql
* reindex
* hide
* gen\_flakes
* query\_with
* test\_transact\_with
* block\_range\_with
* ledger\_stats
* storage
* pw
  * generate
  * renew
  * login

If you feel any of these requires priority, please [submit an issue](https://github.com/pibara/aioflureedb/issues) or fork and submit a fork request.
