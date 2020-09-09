## Low level API

The low-level API is used for signing transactions and queries.
Please use the [high-level API](API.md) whenever possible instead.


### Instantiating a DbSigner 

```python
from aioflureedb.signing import DbSigner

..

privkey = "bf8a7281f43918a18a3feab41d17e84f93b064c441106cf248307d87f8a60453"
address = "1AxKSFQ387AiQUX6CuF3JiBPGwYK5XzA1A"
database = "dev/main"
signer = DbSigner(privkey, address, database)

```

The DbSigner takes two optional aditional parameters

```python
..
signer = DbSigner(privkey, address, database, validity=300, fuel=1234)
```

### Signing a transaction

```python
transaction = [{"foo": 42, "bar": "appelvlaai"}]
command = signer.sign_transaction(transaction)
command_json = json.dumps(command, indent=4, sort_keys=True) 
```

The sign\_transaction takes an optional *deps* argument as described in the fluree documentation on transactions.

### Signing a query

```python
query = {"foo": 42, "bar": "appelvlaai"}
body, headers, uri = signer.sign_query(query)
```

The sign\_query method takes an optional *querytype* argument (default is "query").
