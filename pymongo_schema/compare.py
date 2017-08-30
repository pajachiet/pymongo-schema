# coding: utf8
"""
This module intends to compare two mongo schemas (from extract module)
"""


def compare_schemas_bases(schema, exp_schema, hierarchy=''):
    """
    Recursively compare the base structure of two schemas.

    The base structure compared is:
    - fields (or collection or database) names
    - fields type (keys 'type' and 'array_type')

    It displays only the first difference.
    If a field appears in one schema but not in the other,
    the field is displayed but not its entire data.

    example:
    with inputs:
    schema = {'db': {'coll': {'object': {'field1': {'type': 'string'}}}}}
    exp_schema = {'db': {'coll': {'object': {'field1': {'type': 'integer'}}}}}

    result will be:
    [{'hierarchy': 'db.coll',
      'schema': {'field1': {'type': 'string'}},
      'exp_schema': {'field1': {'type': 'integer'}}}]

    :param schema: dict - schema to be compared
    :param exp_schema: dict - schema to compare to
    :param hierarchy: string - describe level of recursion (keep tracks of previous levels)
    :return: list of dicts describing differences
                [{'hierarchy': db_name.coll_name,
                  'schema': differing_value_in_schema,
                  'expected': differing_value_in_exp_schema}]
    """
    diff = []

    additional_fields = set(schema) - set(exp_schema)
    missing_fields = set(exp_schema) - set(schema)

    # manage additional / missing fields
    for field in additional_fields:
        diff.append({'hierarchy': hierarchy, 'schema': field, 'expected': None})
    for field in missing_fields:
        diff.append({'hierarchy': hierarchy, 'schema': None, 'expected': field})

    # manage differences
    for field in sorted(set(schema) & set(exp_schema)):
        # manage initial case: field is db name and values are collections (not fields yet)

        if not hierarchy:
            diff += compare_schemas_bases(schema[field], exp_schema[field], hierarchy=field)

        # manage regular case (differences of fields type)
        if schema[field].get('type') != exp_schema[field].get('type'):
            diff.append({'hierarchy': '{}.{}'.format(hierarchy, field),
                         'schema': {'type': schema[field]['type']},
                         'expected': {'type': exp_schema[field]['type']}})

        # manage array case (differences of fields array_type) only if both types are ARRAY
        elif schema[field].get('array_type') != exp_schema[field].get('array_type'):
            diff.append({'hierarchy': '{}.{}'.format(hierarchy, field),
                         'schema': {'array_type': schema[field]['array_type']},
                         'expected': {'array_type': exp_schema[field]['array_type']}})

        # recursion in case of nested object
        if 'object' in schema[field] and 'object' in exp_schema[field]:
            diff += compare_schemas_bases(schema[field]['object'],
                                          exp_schema[field]['object'],
                                          hierarchy='{}.{}'.format(hierarchy, field))
    return diff
