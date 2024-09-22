#!/bin/bash

if [[ "$#" -eq 0 ]]; then
    alias a="bash sourceme.sh run"
    alias a
else
    rm -f yes.txt.zst
    set -o xtrace
    python3 recompress.py yes.txt.gz
fi
