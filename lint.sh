#!/bin/bash -x
pylint aioflureedb/__init__.py | tail -2| head -1
pycodestyle aioflureedb/__init__.py --max-line-length=128
pylint aioflureedb/signing.py | tail -2 | head -1
pycodestyle aioflureedb/signing.py --max-line-length=128

