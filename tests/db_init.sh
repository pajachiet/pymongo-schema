#!/usr/bin/env bash

# This script assumes that a MongoDB instance is currently running.
# The MongoDB instance is populated with a json dataset.
DB_DIRS=tests/resources/functional/input_dbs/*
for db in ${DB_DIRS}
do
echo "Processing $db directory"
DB_NAME=$(basename ${db})
for f in $(ls ${db})
do
  echo "Processing ${db}/${f} file"
  FILE_NAME=$(basename ${f})
  mongoimport --db ${DB_NAME} --collection "${FILE_NAME/.json/}" --drop --file ${db}/${f}
done
done

