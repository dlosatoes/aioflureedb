#!/usr/bin/python3
import json

def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]

class KeySetter:
    def __init__(self, query, key):
        self.query = query
        self.key = key
    def __call__(self,*args,**kwargs):
        if args:
            if len(args) == 1:
                val = args[0]
            else:
                val = args
        else:
            val - dict()
            for key, value in kwargs.items():
                val[key] = value
        self.query.obj[self.key] = val
        return self.query

class FlureeQuery:
    def __init__(self, printer):
        self.printer=printer
        self.obj = dict()
        self.valid = set(["select","selectOne","selectDistinct","from", "where","block","prefixes","vars","opts"])
    def __call__(self, obj):
        self.printer.print_it(obj)
    def execute(self):
        self.printer.print_it(self.obj)
    def __getattr__(self, key):
        key = remove_prefix(key,"fluree_")
        if key not in self.valid:
            raise AttributeError("OOPS "+ key)
        return KeySetter(self,key)


class FlureePrinter:
    def __getattr__(self, method):
        if method == "query":
            return FlureeQuery(self)
        else:
            raise AttributeError("OOPS")
    def print_it(self, obj):
        print(json.dumps(obj))

printer = FlureePrinter()
printer.query({"select" : ["_id", "name"], "from" : "_auth"})
# printer.query(select=["_id", "name"], from= "_auth"})
printer.query.select("_id", "name").fluree_from("_auth").execute()

try:
    printer.hohoho()
except:
    print("No hohoho")
try:
    printer.query.select("_id", "name").fluree_from("_auth").hoho("not good").execute()
except:
    print("No hoho")

