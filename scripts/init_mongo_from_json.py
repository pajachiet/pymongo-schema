#!/usr/bin/env python3
import os
import json
from bson.json_util import loads as bson_loads
from pymongo import MongoClient

ROOT = os.path.dirname(os.path.dirname(__file__))
INPUT_DIR = os.path.join(ROOT, 'tests', 'resources', 'functional', 'input_dbs')


def import_db(db_path, client):
    db_name = os.path.basename(db_path)
    db = client[db_name]
    for fname in sorted(os.listdir(db_path)):
        if not fname.endswith('.json'):
            continue
        col_name = fname[:-5]
        col = db[col_name]
        # Drop existing collection to match tests/db_init.sh behavior
        col.drop()
        fpath = os.path.join(db_path, fname)
        docs = []
        with open(fpath, 'r', encoding='utf-8') as f:
            content = f.read()
        content = content.strip()
        if not content:
            docs = []
        else:
            try:
                loaded = bson_loads(content)
                if isinstance(loaded, list):
                    docs = loaded
                else:
                    docs = [loaded]
            except json.JSONDecodeError:
                # Fallback: concatenated JSON objects without separators
                decoder = json.JSONDecoder()
                idx = 0
                length = len(content)
                while idx < length:
                    # Skip whitespace
                    while idx < length and content[idx].isspace():
                        idx += 1
                    if idx >= length:
                        break
                    # Use standard decoder for raw chunks then pass through bson_loads
                    obj_str, end = decoder.raw_decode(content, idx)
                    obj = bson_loads(json.dumps(obj_str))
                    docs.append(obj)
                    idx = end
        if docs:
            col.insert_many(docs)


def main():
    client = MongoClient('localhost', 27017)
    for entry in sorted(os.listdir(INPUT_DIR)):
        path = os.path.join(INPUT_DIR, entry)
        if os.path.isdir(path):
            import_db(path, client)


if __name__ == '__main__':
    main()
