#!/bin/bash
echo "Testing transaction signing 6 times (python -> javascript)"
for i in `seq 6`; do ./fluree/transaction-generate.py |./fluree/transaction-validate.js ; done
echo "Testing query signing 6 times (python -> javascript)"
for i in `seq 6`; do ./fluree/query-generate.py |./fluree/query-validate.js ; done 
echo "Testing transaction signing 6 times (javascript -> javascript)"
for i in `seq 6`; do ./fluree/transaction-reference.js |./fluree/transaction-validate.js ; done
echo "Testing query signing 6 times (javascript -> javascript)"
for i in `seq 6`; do ./fluree/query-reference.js |./fluree/query-validate.js ; done
