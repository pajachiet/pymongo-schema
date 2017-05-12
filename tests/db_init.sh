#!/usr/bin/env bash

# This script starts a MongoDB instance to run the tests locally.
# The MongoDB instance in populated with a json dataset.

db_directory="tests/mongodb"

if [ ! -d "$db_directory" ]; then
    mkdir $db_directory
fi

# Start mongod as a service
mongod --fork --dbpath $db_directory --logpath "$db_directory/mongodb.log"

# Import test dataset
mongoimport --db test_db --collection test_col --drop --file "tests/test_db.json"

