## aioflureedb

### dependency notes

Please note, this library depends on a [patch](https://github.com/starkbank/ecdsa-python/pull/21) to the [StarkBank ECDSA library](https://pypi.org/project/starkbank-ecdsa/) that is currently not merged into master yet. You will need to pull the library from the **pibara** fork instead for now.

If you already have the library installed, uninstall it:

```bash
python3 -m pip uninstall starkbank-ecdsa
```

Then install with the fork of the dependency.

```bash
python3 -m pip install . --process-dependency-links
```

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

### Signing a query
This is currently an untested part of the library.

```python
query = {"foo": 42, "bar": "appelvlaai"}
body, headers, uri = signer.sign_query(query)
```
### High level API

The high-level API is an API for doing asynchonous queries and transactions as a FlureeDB client. The high level API doesn't exist yet.
