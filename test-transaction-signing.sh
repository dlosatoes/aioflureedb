#!/bin/bash
for i in `seq 10`; do fluree/transaction-generate.py |./fluree/transaction-validate.js ; done

