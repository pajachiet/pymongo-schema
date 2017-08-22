#!/usr/bin/env bash

# This script starts a MongoDB instance to run the tests locally.
# The MongoDB instance in populated with a json dataset.

db_directory="tests/mongodb"

if [ ! -d "$db_directory" ]; then
    mkdir $db_directory
fi

# Start mongod as a service
#mongod --fork --dbpath $db_directory --logpath "$db_directory/mongodb.log"

# Import test dataset
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

