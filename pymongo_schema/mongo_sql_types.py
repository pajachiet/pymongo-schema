# coding: utf8

import bson
from ete3 import Tree

TYPE_TO_STR = {
    list: "ARRAY",
    dict: "OBJECT",
    type(None): "null",

    bool: "boolean",
    int: "integer",
    bson.int64.Int64: "biginteger",
    float: "float",

    str: "string",
    unicode: "string",

    bson.datetime.datetime: "date",
    bson.timestamp.Timestamp: "timestamp",

    bson.dbref.DBRef: "dbref",
    bson.objectid.ObjectId: "oid",
}


def type_name(value):
    """Return mongo type string from a value 
    
    :param value: 
    :return type_str: str
    """
    return TYPE_TO_STR[type(value)]  # TODO : handle UNDEFINED types


TYPES_TREE_STR = """
(
    (
        (
            float, 
            ((boolean) integer) biginteger
        ) number,
        (
            oid, 
            dbref
        ) str,
        date,
        timestamp
    ) general_scalar,
    object
) mixed_scalar_object
;"""

TYPES_TREE = Tree(TYPES_TREE_STR, format=8)


def least_common_parent_type(type_list):
    """Get the least common parent type from a list of types.

    :param type_list: list
    :return common_type: type_str
    """
    if not type_list:
        return 'null'
    elif len(type_list) == 1:
        return type_list[0]
    else:
        return TYPES_TREE.get_common_ancestor(*type_list).name


def get_tree_style():
    """Custom tree style to output type tree figure."""
    ts = TreeStyle()
    ts.show_leaf_name = False
    ts.show_scale = False
    ts.orientation = 1
    ts.branch_vertical_margin = 20

    def my_layout(node):
        F = TextFace(node.name, fsize=16, ftype='Courier', bold=True)
        faces.add_face_to_node(F, node, column=10, position="branch-right")
    ts.layout_fn = my_layout
    return ts

MONGO_TO_PSQL_TYPE = {
    'boolean': 'BOOLEAN',
    'integer': 'INT',
    'biginteger': 'BIGINT',
    'float': 'REAL',
    'number': 'DOUBLE PRECISION',
    'date': 'TIMESTAMP',
    'string': 'TEXT',
    'oid': 'TEXT',
}


def psql_type(mongo_type_str):
    """Map a MongoDB type string to a PSQL type string
    
    :param mongo_type_str: str
    :return psql_type_str: str
    """
    psql_type_str = MONGO_TO_PSQL_TYPE[mongo_type_str]
    return psql_type_str

if __name__ == '__main__':
    from ete3 import faces, TextFace, TreeStyle

    # Generate type_tree image. It needs ETE dependencies installed
    # cf http://etetoolkit.org/new_download/ or use anaconda
    TYPES_TREE.render("type_tree.png", tree_style=get_tree_style());