#!/bin/bash

if [[ "$#" -eq 0 ]] ; then
    alias a="bash sourceme.sh run"
    alias a
else
    rm yes.txt.gz.zst
    python3 recompress.py yes.txt.gz
fi
