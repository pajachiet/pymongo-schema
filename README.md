# pymongo-schema
A schema analyser for MongoDB, written in Python. 

This tools is inspired by [variety](https://github.com/variety/variety), with the following enhancements :

- extract the **hierarchical structure** of the schema 
- Several output options : python dictionnary, json, yaml or text
- **finer grained types**. ex: INTEGER, DOUBLE rather than NUMBER 
- ways to **filter** and **manipulate** the output schema

# Install

Before distribution of a stable distribution on PyPi, you can install pymongo-schema from github : 
```shell
pip install -e git+https://github.com/pajachiet/pymongo-schema.git#egg=Package
```
# Usage

```shell
pymongo-schema  [--database=DB --collection=COLLECTION... --output=FILENAME --format=FORMAT... --port=PORT --host=HOST]
```

To display full usage, run
```shell 
pymongo-schema -h
```

TODO : add examples

# Contributing - Limitations - TODO 
The code base should be easy to read and improve upon. Contributions are welcomed.

## Mixed types handling
Currently, pymongo-schema does not handle inconsistent types in a field. It only check consistency and raise an exception in case of a problem.

- Improve mapping from Python type to name (TYPE_TO_STR dict)
    - see documentation: [bson-types](https://docs.mongodb.com/manual/reference/bson-types/), [spec](http://bsonspec.org/spec.html)

- Raise a proper error in case of inconsistent types and catch it above, to treat it or raise it with more context information (key, type, value…)

- Options to handle mixed-types
   - Use a more general type, ex INT < LONG < NUMERIC
   - Allow a field to contains either a type or an array of that type
   - Store a list of types for a field, eventually with its count 

## Logging
- Log database and collection being processed
- Log "percent complete"  / number of document 

## Diff between schemas

A way to compare the schema dictionaries and highlights the differences.


## Tackle bigger databases
This code has been only used on a relatively small sized Mongo database, on which it was faster than Variety. 

To tackle bigger databases, it certainly would be usefull to implement the following variety's features :

- Analyze subsets of documents, most recent documents, or documents to a maximum depth.

## Tests
The codebase is not tested. It should not be trusted blindly.

## Distribution

Distribute in PyPi

## Support multiple Python version

And test for it


