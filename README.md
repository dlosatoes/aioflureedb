## aioflureedb

This library isn't production stable yet. Please only use for experiments and development hints.

### Install


```bash
git clone https://github.com/pibara/aioflureedb.git
cd aioflureedb
python3 -m pip install . --process-dependency-links
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

#### Instantiating a FlureeDB client

```python
import asyncio
import aioflureedb

async def main(clnt):
    ...

privkey = "bf8a7281f43918a18a3feab41d17e84f93b064c441106cf248307d87f8a60453"
address = "1AxKSFQ387AiQUX6CuF3JiBPGwYK5XzA1A"
database = "myorg/mydb"
client = aioflureedb.FlureeDbClient(privkey, address, database, port=8090)
loop = asyncio.get_event_loop()
loop.run_until_complete(main(client))

```

#### Using the command API endpoint for transactions

```python
async def main(clnt):
     try:
        transaction = await client.command.transaction([{"_id":"_user","username": randomuser}])
        print("OK: TRANSACTION STARTED:", transaction)
    except Exception as exp:
        print("FAIL: TRANSACTION FAILED:", exp)
    ...
    await clnt.close_session()
```

Note! an await on transaction resolves as soon as the transaction has been submitted. An other method should be made that only resolves when the transaction has actually completed.


#### Using the query API endpoint for flureeql queries

```python
async def main(clnt):
     try:
        result = await client.query.query({"select": ["*"],"from": "_user"})
        print("OK: QUERY SUCCEDED:", result)
    except Exception as exp:
        print("FAIL: QUERY FAILED:", exp)
    ...
    await clnt.close_session()
```

NOTICE: This method is currently broken! Work In Progress.
