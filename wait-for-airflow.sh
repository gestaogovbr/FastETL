#!/bin/bash
while [[ "$(curl --silent 'http://localhost:8080/health' \
    | python3 -c 'import sys, json; print(json.load(sys.stdin)["scheduler"]["status"])')" != "healthy" ]];
do {
    printf "."; sleep 2;  
} done