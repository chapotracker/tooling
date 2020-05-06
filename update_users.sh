#!/bin/bash

LIST_DIR=../chapolist
TS=$(date +"%Y-%m-%d %H:%M:%S")

python ./extract_data.py -o $LIST_DIR/users.json
cd $LIST_DIR
git add users.json
git commit -m "Updating user list with users as of $TS"
git push
