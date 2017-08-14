# coding: utf8

"""
Module grouping all TYPE's related issues
 - mapping from pymongo_type to type_string
    - used in extract
    - to be completed

 - type_string_tree, to get the least common parent type_string from a list of type_string
    - used in extract while post-processing
    - to be refactored

 - mapping from type_string to psql_type
    - used while mapping mongo_schema tosql
    - to be completed
"""

import logging

import bson
from ete3 import Tree

logger = logging.getLogger(__name__)

###
# Mapping from pymongo_type to type_string

PYMONGO_TYPE_TO_TYPE_STRING = {
    list: "ARRAY",
    dict: "OBJECT",
    type(None): "null",

    bool: "boolean",
    int: "integer",
    bson.int64.Int64: "biginteger",
    float: "float",

    str: "string",

    bson.datetime.datetime: "date",
    bson.timestamp.Timestamp: "timestamp",

    bson.dbref.DBRef: "dbref",
    bson.objectid.ObjectId: "oid",
}

try:
    PYMONGO_TYPE_TO_TYPE_STRING[unicode] = 'string'
except NameError:
    pass

def get_type_string(value):
    """ Return mongo type string from a value

    :param value:
    :return type_string: str
    """
    value_type = type(value)
    try:
        type_string = PYMONGO_TYPE_TO_TYPE_STRING[value_type]
    except KeyError:
        logger.warning("Pymongo type %s is not mapped to a type_string. "
                       "We define it as 'unknown' for current schema extraction", value_type)
        PYMONGO_TYPE_TO_TYPE_STRING[value_type] = 'unknown'
        type_string = 'unknown'

    return type_string


###
# Define and use type_string_tree,
# to get the least common parent type_string from a list of type_string

NEWICK_TYPES_STRING_TREE = """
(
    (
        (
            float, 
            ((boolean) integer) biginteger
        ) number,
        (
            oid, 
            dbref
        ) string,
        date,
        timestamp,
        unknown
    ) general_scalar,
    OBJECT
) mixed_scalar_object
;"""

TYPES_STRING_TREE = Tree(NEWICK_TYPES_STRING_TREE, format=8)


def common_parent_type(list_of_type_string):
    """ Get the common parent type from a list of types.

    :param list_of_type_string: list
    :return common_type: type_str
    """
    if not list_of_type_string:
        return 'null'
    # avoid duplicates as get_common_ancestor('integer', 'integer') -> 'number'
    list_of_type_string = list(set(list_of_type_string))
    if len(list_of_type_string) == 1:
        return list_of_type_string[0]
    return TYPES_STRING_TREE.get_common_ancestor(*list_of_type_string).name


def generate_type_tree_figure(output_file):
    """ Generate type_tree.png image.

    It needs ETE dependencies installed
    cf http://etetoolkit.org/new_download/ or use anaconda

    :param output_file: str
    """
    try:
        from ete3 import faces, TextFace, TreeStyle
    except ImportError as e:
        logger.warning('ImportError : %s Generation of type_tree figure need ETE dependencies to '
                       'be installed Use from anaconda, or look at installation procedure on '
                       'http://etetoolkit.org/new_download/', e)
        return

    # Define custom tree style
    ts = TreeStyle()
    ts.show_leaf_name = False
    ts.show_scale = False
    ts.orientation = 1
    ts.branch_vertical_margin = 20

    def my_layout(node):
        F = TextFace(node.name, fsize=16, ftype='Courier', bold=True)
        faces.add_face_to_node(F, node, column=10, position="branch-right")

    ts.layout_fn = my_layout

    TYPES_STRING_TREE.render(output_file, tree_style=ts)


###
# Mapping from type_string to psql_type


MONGO_TO_PSQL_TYPE = {
    'boolean': 'BOOLEAN',
    'integer': 'INT',
    'biginteger': 'BIGINT',
    'float': 'REAL',
    'number': 'DOUBLE PRECISION',
    'date': 'TIMESTAMP',
    'string': 'TEXT',
    'oid': 'TEXT',
    'dbref': 'TEXT'
}


def psql_type(mongo_type_str):
    """ Map a MongoDB type string to a PSQL type string

    :param mongo_type_str: str
    :return psql_type_str: str
    """
    psql_type_str = MONGO_TO_PSQL_TYPE[mongo_type_str]
    return psql_type_str


if __name__ == '__main__':
    logging.basicConfig()
    generate_type_tree_figure("type_tree.png")
