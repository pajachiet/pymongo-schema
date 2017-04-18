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
pip install --upgrade -e git+https://github.com/pajachiet/pymongo-schema.git#egg=Package
```
# Usage

```shell
Usage:
    pymongo-schema  -h | --help
    pymongo-schema  extract [--database=DB --collection=COLLECTION... --output=FILENAME --format=FORMAT... --port=PORT --host=HOST --quiet]
    pymongo-schema  filter --input=FILENAME --namespace=FILENAME [--output=FILENAME --format=FORMAT... --quiet]
    pymongo-schema  tosql --input=FILENAME [--output=FILENAME --quiet]


Commands: 
    extract                     Extract schema from a MongoDB instance
    filter                      Apply a namespace filter to a mongo schema
    tosql                       Create a mapping from mongo schema to relational schema
```

To display full usage, with options description, run:
```shell 
pymongo-schema -h
```

TODO : add examples

# Schema

We define 'schema' as a dictionnary describing the structure of MongoDB component, being either a MongoDB instances, a database, a collection, an objects or a field. 
 
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
                    'types_count': # count for each encountered type  
                    {
                        'type_str' : 13,
                        'Null' : 3
                    }, 

                    'array_types_count': # (optional) count for each type encountered  in arrays
                    {
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
Currently, pymongo-schema does not handle inconsistent types in a field. It only check consistency and raise an exception in case of a problem.

- Improve mapping from Python type to name (TYPE_TO_STR dict)
    - see documentation: [bson-types](https://docs.mongodb.com/manual/reference/bson-types/), [spec](http://bsonspec.org/spec.html)

- Raise a proper error in case of inconsistent types and catch it above, to treat it or raise it with more context information (key, type, value…)

- Options to handle mixed-types
   - Use a more general type, ex INT < LONG < NUMERIC
   - Allow a field to contains either a type or an array of that type
   - Store a list of types for a field, eventually with its count 

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


