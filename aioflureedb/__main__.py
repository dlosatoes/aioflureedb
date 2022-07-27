import sys
import asyncio
import argparse
import json
from . import FlureeClient
from .domain_api import FlureeDomainAPI

async def fluree_main(client, endpoint, data):
    if endpoint is None:
        print("No endpoint specified, default to health")
        endpoint="health"
    if endpoint == "health":
        return await client.health()
    await client.health.ready()
    if endpoint == "dbs":
        return await client.dbs()
    if endpoint == "new_keys":
        return await client.new_keys()
    if endpoint == "version":
        return await client.version()
    if endpoint == "new_db":
        print("data: '", data , "'")
        return await client.new_db(db_id=data)
    return "Unknown endpoint:" + str(endpoint) 

async def database_main(client, db, endpoint, data, key):
    await client.health.ready()
    fdb = await client[db]
    async with fdb(key) as database:
        await database.ready()
        if endpoint == "ledger_stats":
            return await database.ledger_stats()
        if endpoint == "flureeql":
            return await database.flureeql.query.raw(json.loads(data))
        if endpoint == "history":
            return await database.history.actual_query(json.loads(data))
        if endpoint == "block":
            return await database.block.actual_query(json.loads(data))
        if endpoint == "command":
            return await database.command.transaction(json.loads(data))
    return "Unknown endpoint:" + str(endpoint)

async def argparse_main():
    parsers = {}
    parsers["main"] = argparse.ArgumentParser()
    subparsers = parsers["main"].add_subparsers()
    subcommands = ["fluree", "database"]
    helps = {
        "fluree": "Invoke fluree server level API endpoint",
        "database": "Invoke database level API endpoint",
        "masterkey": "info",
        "host": "fluree server host name (default localhost)",
        "port": "Port used by flureedb server (default 8090)",
        "https": "Use https or not (default false)",
        "sslverify": "Verify ssl whe nusing https (default true)",
        "sigvalidity": "Validity of ECDSA signature",
        "sigfuel": "undocumented",
        "endpoint": "FlureeDB server endpoint",
        "data": "Payload as string",
        "datafile": "Payload from file",
        "db": "The network/database name string for the database to use"
    }
    defaults = {
        "host": "localhost",
        "port": "8090",
        "https": "false",
        "sslverify": "true",
        "sigvalidity": "120",
        "sigfuel": "1000",
        "masterkey": None,
        "endpoint": None,
        "datafile": None,
        "data": None,
        "db": None
    }
    argsmap = {
            "fluree": {
                "masterkey",
                "host",
                "port",
                "https",
                "sslverify",
                "sigvalidity",
                "sigfuel",
                "endpoint",
                "data",
                "datafile"
            },
            "database": {
                "masterkey",
                "host",
                "port",
                "https",
                "sslverify",
                "sigvalidity",
                "sigfuel",
                "endpoint",
                "data",
                "datafile",
                "db"
            },

    }
    for subcommand in subcommands:
        sc_help = helps[subcommand]
        sc_args = argsmap[subcommand]
        parsers[subcommand] = subparsers.add_parser(subcommand, help=sc_help)
        parsers[subcommand].add_argument('--subcommand', help=argparse.SUPPRESS, default=subcommand)
        for sc_arg in sc_args:
            for subarg in sc_arg.split(":"):
                sa_help = helps[subarg]
                if subarg in defaults:
                    sa_default = defaults[subarg]
                    if sa_default is not None and not sa_default:
                        parsers[subcommand].add_argument('--' + subarg,
                                                         action='store_true',
                                                         help=sa_help)
                    else:
                        parsers[subcommand].add_argument("--" + subarg,
                                                         help=sa_help,
                                                         default=sa_default)
                else:
                    parsers[subcommand].add_argument(subarg, help=sa_help)
    args = parsers["main"].parse_args()
    if not vars(args):
        print("Please supply commandline agruments. Use --help for info")
        sys.exit(1)
    data = args.data
    if data is None:
        data = ""
        if args.datafile is not None:
            if args.datafile == "-":
                inf = sys.stdin
            else:
                inf = open(args.datafile)
            data = inf.read()
    async with FlureeClient(args.masterkey,
                          args.host,
                          int(args.port),
                          args.https != "false",
                          args.sslverify != "false",
                          float(args.sigvalidity),
                          float(args.sigfuel)) as client:
        try:
            if args.subcommand == "fluree":
                print(json.dumps(await fluree_main(client, args.endpoint, data), indent=2))
            elif args.subcommand == "database":
                print(json.dumps(await database_main(client, args.db, args.endpoint, data, args.masterkey), indent=2))
        except Exception as exp:
            print(exp)


LOOP = asyncio.get_event_loop()
LOOP.run_until_complete(argparse_main())

