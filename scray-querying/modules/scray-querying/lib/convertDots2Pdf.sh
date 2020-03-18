#!/bin/bash

# assumes graphviz installed and on path
for i in /tmp/scray*.dot; do
    dot -Tpdf $i -o $i.pdf
done
