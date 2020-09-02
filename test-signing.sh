#!/bin/bash
echo "Testing transaction signing 10 times"
for i in `seq 10`; do ./fluree/transaction-generate.py |./fluree/transaction-validate.js ; done
echo "Testing query signing 10 times"
for i in `seq 10`; do ./fluree/query-generate.py |./fluree/query-validate.js ; done 
