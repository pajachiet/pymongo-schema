# pymongo-schema
A schema analyser for MongoDB, written in Python. 

This tool allows you to **extract your application's schema, directly from your MongoDB data**. It comes with **powerful schema manipulation and export functionalities**.

It will be particularly useful when you inherit a data dump, and want to quickly learn how the data is structured. 

pymongo-schema allows to map your MongoDB data model to a relational (SQL) data model. This greatly helps to configure [mongo-connector-postgresql](https://github.com/Hopwork/mongo-connector-postgresql), a tool to synchronize data from MongoDB to a target PostgreSQL database.

It also helps you to **compare different versions of your data model**.

This tools is inspired by [variety](https://github.com/variety/variety), with the following enhancement

- extract the **hierarchical structure** of the schema 
- versatile output options : json, yaml, tsv, markdown or htlm
- **finer grained types**. ex: INTEGER, DOUBLE rather than NUMBER 
- **filtering** of the output schema
- **mapping to a relational schema**
- **comparison** of successive schema

[![Build Status](https://travis-ci.org/pajachiet/pymongo-schema.svg?branch=master)](https://travis-ci.org/pajachiet/pymongo-schema)
[![Coverage Status](https://coveralls.io/repos/github/pajachiet/pymongo-schema/badge.svg?branch=master)](https://coveralls.io/github/pajachiet/pymongo-schema?branch=master)


# Install

Before distribution of a stable distribution on PyPi, you can install pymongo-schema from github : 
```shell
pip install --upgrade https://github.com/pajachiet/pymongo-schema/archive/master.zip
```
# Usage

```shell
python -m pymongo_schema -h
usage: [-h] [--quiet] {extract,transform,tosql,compare} ...

commands:
  {extract,transform,tosql,compare}
    extract             Extract schema from a MongoDB instance
    transform           Transform a json schema to another format, potentially
                        filtering or changing columns outputs
    tosql               Create a mapping from mongo schema to relational
                        schema (json input and output)
    compare             Compare two schemas

optional arguments:
  -h, --help            show this help message and exit
  --quiet               Remove logging on standard output

Usage:
    python -m pymongo_schema extract -h
    usage:  [-h] [-f [FORMATS [FORMATS ...]]] [-o OUTPUT] [--port PORT] [--host HOST]
                 [-d [DATABASES [DATABASES ...]]] [-c [COLLECTIONS [COLLECTIONS ...]]]
                 [--columns COLUMNS [COLUMNS ...]] [--without-counts]
                 
    python -m pymongo_schema transform -h
    usage: [-h] [-f [FORMATS [FORMATS ...]]] [-o OUTPUT] [--category CATEGORY] [-n FILTER]
                [--columns COLUMNS [COLUMNS ...]] [--without-counts] [input]
                
    python -m pymongo_schema tosql -h
    usage: [-h] [-f [FORMATS [FORMATS ...]]] [--columns COLUMNS [COLUMNS ...]]
                [--without-counts] [-o OUTPUT] [input]

    python -m pymongo_schema compare -h
    usage: [-h] [-f [FORMATS [FORMATS ...]]] [-o OUTPUT] [input]
                [--columns COLUMNS [COLUMNS ...]] [--without-counts]
                [--detailed_diff] prev_schema [new_schema]

```

To display full usage, with options description, run:
```shell 
pymongo-schema <command> -h
```

# Easy examples

First, lets populate a collection in test database from mongo shell


    db.users.insert({name: "Tom", bio: "A nice guy.", pets: ["monkey", "fish"], someWeirdLegacyKey: "I like Ike!"});
    db.users.insert({name: "Dick", bio: "I swordfight.", birthday: new Date("1974/03/14")});
    db.users.insert({name: "Harry", pets: "egret", birthday: new Date("1984/03/14"), location:{country:"France", city: "Lyon"}});
    db.users.insert({name: "Geneviève", bio: "Ça va?", location:{country:"France", city: "Nantes"}});
    db.users.insert({name: "MadJacques", location:{country:"France", city: "Paris"}});


Extract the schema from this database, with a markdown format on standard output

    $ python -m pymongo_schema extract --database test --format md
    Extract schema of database test
    ...collection users
       scanned 5 documents out of 5 (100.00 %)
    
    ### Database: test
    #### Collection: users 
    |Field_compact_name     |Field_name             |Count     |Percentage     |Types_count                           |
    |-----------------------|-----------------------|----------|---------------|--------------------------------------|
    |_id                    |_id                    |5         |100.0          |oid : 5                               |
    |name                   |name                   |5         |100.0          |string : 5                            |
    |bio                    |bio                    |3         |60.0           |string : 3                            |
    |location               |location               |3         |60.0           |OBJECT : 3                            |
    | . city                |city                   |3         |100.0          |string : 3                            |
    | . country             |country                |3         |100.0          |string : 3                            |
    |birthday               |birthday               |2         |40.0           |date : 2                              |
    |pets                   |pets                   |2         |40.0           |ARRAY(string : 2) : 1, string : 1     |
    |someWeirdLegacyKey     |someWeirdLegacyKey     |1         |20.0           |string : 1                            |

Extract the same schema to a file in json format.


Filter this schema

Map this schema to a relational mapping

# Examples

extract:
```shell
    python -m pymongo_schema extract --databases test_db --collections test_collection_1 test_collection_2 --output mongo_schema --format html json
```
transform:
```shell
    python -m pymongo_schema transform mongo_schema.json --filter namespace.json --output mongo_schema_filtered --format html csv json
```
tosql:
```shell
    python -m pymongo_schema tosql mongo_schema_filtered.json --output mapping.json
```

# Schema

We define 'schema' as a dictionary describing the structure of MongoDB component, being either a MongoDB instances, a database, a collection, an objects or a field. 
 
Schema are hierarchically nested, with the following structure :  



```python 
# mongo_schema : A MongoDB instance contains databases
{
    "database_name_1": {}, #database_schema,
    "database_name_2": # A database contains collections
    { 
        "collection_name_1": {}, # collection_schema,
        "collection_name_2": # A collection maintains a 'count' and contains 1 object
        { 
            "count" : int, 
            "object":  # object_schema : An object contains fields.            
             {
                "field_name_1" : {}, # field_schema, 
                "field_name_2": # A field maintains 'types_count_information
                                # An optional 'array_types_count' field maintains 'types_count' information for values encountered in arrays 
                                # An 'OBJECT' or 'ARRAY(OBJECT)' field recursively contains 1 'object'
                {
                    'count': int,
                    'prop_in_object': float,
                    'type': 'type_str', 
                    'types_count': {  # count for each encountered type  
                        'type_str' : 13,
                        'Null' : 3
                    }, 
                    'array_type': 'type_str',
                    'array_types_count': {  # (optional) count for each type encountered  in arrays
                        'type_str' : 7,
                        'Null' : 3
                    }, 
                    'object': {}, # (optional) object_schema 
                } 
            } 
        }
    }           
}
```
# Contributing - Limitations - TODO 
The code base should be easy to read and improve upon. Contributions are welcomed.

## Mixed types handling
pymongo-schema handles mixed types by looking for the lowest common parent type in the following tree.

<img src="type_tree.png" alt="type_tree" width=700/>

If a field contains both arrays and scalars, it is considered as an array. The 'array_type' is defined as the common parent type of scalars and array_types encountered in this field. 

TODO

- Improve mapping from Python type to name (TYPE_TO_STR dict)
    - see documentation: [bson-types](https://docs.mongodb.com/manual/reference/bson-types/), [spec](http://bsonspec.org/spec.html)

- Check a mongo scheme for compatibility to an sql mapping
- Handle incompatibilities

## Support Python 3 version

- fix encoding issues when exporting manually added non-ascii characters

## Diff between schemas

A way to compare the schema dictionaries and highlights the differences.


## Test if a mongo schema can be mapped tosql

- test for the presence of mongo types in the mapping 
- look for mixes of list and scalar, that are currently not supported by mongo-connector-postgresql
- look for the presence of an '_id'

=> It may be donne directly in mongo-connector-postgresql doc_manager


## Adding fields in json/yaml outputs

- for example to add comments


## Other option to sort text outputs

- It is currently based on counts and then alphabetically.



## Tackle bigger databases
This code has been only used on a relatively small sized Mongo database, on which it was faster than Variety. 

To tackle bigger databases, it certainly would be usefull to implement the following variety's features :

- Analyze subsets of documents, most recent documents, or documents to a maximum depth.

## Tests
The codebase is still under development. It should not be trusted blindly.

## Distribution

Will be distributed in PyPi




