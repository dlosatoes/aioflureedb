## aioflureedb

This library isn't production stable yet. Please only use for experiments and development hints.

### Install


```bash
git clone https://github.com/pibara/aioflureedb.git
cd aioflureedb
python3 -m pip install .
```

If you wish to run the test scripts, install the FlureeDB Javascript library:

```bash
npm install @fluree/crypto-utils
```

This shouldn't be needed for normal operation.

### Low level API

The low-level API is used for signing transactions and queries. Documentalion will follow.

#### Instantiating a DbSigner 

```python
from aioflureedb.signing import DbSigner

..

privkey = "bf8a7281f43918a18a3feab41d17e84f93b064c441106cf248307d87f8a60453"
address = "1AxKSFQ387AiQUX6CuF3JiBPGwYK5XzA1A"
database = "myorg/mydb"
signer = DbSigner(privkey, address, database)

```

#### Signing a transaction

```python
transaction = [{"foo": 42, "bar": "appelvlaai"}]
command = signer.sign_transaction(transaction)
command_json = json.dumps(command, indent=4, sort_keys=True) 
```

#### Signing a query
This is currently an untested part of the library.

```python
query = {"foo": 42, "bar": "appelvlaai"}
body, headers, uri = signer.sign_query(query)
```
### High level API
The high-level API is an API for doing asynchonous queries and transactions as a FlureeDB client. The high level API doesn't quite work yet.

#### Instantiating a Fluree client

```python
import asyncio
import aioflureedb

async def main(clnt):
    ...

privkey = "bf8a7281f43918a18a3feab41d17e84f93b064c441106cf248307d87f8a60453"
address = "1AxKSFQ387AiQUX6CuF3JiBPGwYK5XzA1A"
database = "myorg/mydb"
client = aioflureedb.FlureeClient(privkey, address, port=8090)
loop = asyncio.get_event_loop()
loop.run_until_complete(main(client))

```

#### Getting a database client from the fluree client

```python
async def main(clnt):
    privkey2 = "..."
    addr2 = "..."
    try:
        network = await clnt["dev"]
        db = network["main"]
        database = db(privkey2, addr2)
    except Exception as exp:
        print("Problem accessing database", exp)

    ...

    await clnt.close_session()
    await database.close_session()
```

#### Using the command API endpoint for transactions

```python
async def main(clnt):

     ...
    try:
        transaction = await database.command.transaction([{"_id":"_user","username": randomuser}])
        print("OK: TRANSACTION COMPLETED:", transaction)
    except Exception as exp:
        print("FAIL: TRANSACTION FAILED:", exp) 
    ...
```

Note! an await on transaction resolves as soon as the transaction has been submitted. An other method should be made that only resolves when the transaction has actually completed.


#### Using the query API endpoint for flureeql queries

```python
async def main(clnt):
    try:
        result = await database.query.query(select=["*"], ffrom="_user")
        print("OK: QUERY SUCCEDED:", result)
    except Exception as exp:
        print("FAIL: QUERY FAILED:", exp)
    ...
    await clnt.close_session()
```
Note the use of **ffrom** instead of **from**, this is a little hack to avoid Python keyword issues with the FlureeQL from keyword.

## Work in progress

The aioflureedb library is currently functioning for the API endpoints that it implements. 
These are:

* health  (fluree client)
* dbs     (fluree client)
* query   (database client)
* command (database client)

Some of the other endpoint may be easy to implement, but are currently not yet supported by the code. Pull request with API-endpoint code are very much welcome, as are github project issues that could help prioritize endpoint support.

The first 0.1 release is planned to contain the API as currently defined, unless my call for feedback results in API changes.

### Roadmap for 0.1 version.

* Get feedback from fluree slack
* Any API changes resulting from feedback
* Implement dir support for __getattr__ based APIs
* Get code to pass all pylint and pycodestyle checks

### Not yet implemented/tested (post 0.1):

##### Fluree Client
* new\_db
* delete\_db
* add\_server
* remove\_server
* new\_keys

##### Database Client
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
