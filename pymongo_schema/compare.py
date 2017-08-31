# coding: utf8
"""
This module intends to compare two mongo schemas (from extract module)
"""


def compare_schemas_bases(prev_schema, new_schema, hierarchy=''):
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
    prev_schema = {'db': {'coll': {'object': {'field1': {'type': 'string'}}}}}
    new_schema = {'db': {'coll': {'object': {'field1': {'type': 'integer'}}}}}

    result will be:
    [{'hierarchy': 'db.coll',
      'prev_schema': {'field1': {'type': 'string'}},
      'new_schema': {'field1': {'type': 'integer'}}}]

    :param prev_schema: dict - previous schema to compare to
    :param new_schema: dict - new schema to be compared
    :param hierarchy: string - describe level of recursion (keep tracks of previous levels)
    :return: list of dicts describing differences
                [{'hierarchy': db_name.coll_name,
                  'prev_schema': differing_value_in_prev_schema,
                  'new_schema': differing_value_in_new_schema}]
    """
    diff = []

    additional_fields = set(new_schema) - set(prev_schema)
    missing_fields = set(prev_schema) - set(new_schema)

    # manage additional / missing fields
    for field in missing_fields:
        diff.append({'hierarchy': hierarchy, 'prev_schema': field, 'new_schema': None})
    for field in additional_fields:
        diff.append({'hierarchy': hierarchy, 'prev_schema': None, 'new_schema': field})

    # manage differences
    for field in sorted(set(prev_schema) & set(new_schema)):
        # manage initial case: field is db name and values are collections (not fields yet)

        if not hierarchy:
            diff += compare_schemas_bases(prev_schema[field], new_schema[field], hierarchy=field)

        # manage regular case (differences of fields type)
        if prev_schema[field].get('type') != new_schema[field].get('type'):
            diff.append({'hierarchy': '{}.{}'.format(hierarchy, field),
                         'prev_schema': {'type': prev_schema[field]['type']},
                         'new_schema': {'type': new_schema[field]['type']}})

        # manage array case (differences of fields array_type) only if both types are ARRAY
        elif prev_schema[field].get('array_type') != new_schema[field].get('array_type'):
            diff.append({'hierarchy': '{}.{}'.format(hierarchy, field),
                         'prev_schema': {'array_type': prev_schema[field]['array_type']},
                         'new_schema': {'array_type': new_schema[field]['array_type']}})

        # recursion in case of nested object
        if 'object' in prev_schema[field] and 'object' in new_schema[field]:
            diff += compare_schemas_bases(prev_schema[field]['object'],
                                          new_schema[field]['object'],
                                          hierarchy='{}.{}'.format(hierarchy, field))
    return diff


def is_retrocompatible(diff):
    for line in diff:
        if line['prev_schema'] is not None and line['new_schema'] is not None:
            return False
    return True
