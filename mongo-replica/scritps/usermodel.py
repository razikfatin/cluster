import argparse
import sys
from datetime import datetime
from pprint import pprint

try:
    from pymongo import MongoClient, errors
    from bson import json_util
except Exception as e:
    print("ERROR: pymongo not installed in this Python environment.")
    print("Please install it with: pip install pymongo")
    raise SystemExit(1)


DEFAULT_URI = "mongodb://localhost:27019"
DEFAULT_DB = "mydb"
COLLECTION_NAME = "user_profiles"

VALIDATOR = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["user_id", "username", "email", "created_at", "is_active"],
        "properties": {
            "user_id": {"bsonType": "string"},
            "username": {"bsonType": "string", "description": "must be a string"},
            "email": {"bsonType": "string", "pattern": "^.+@.+\\..+$", "description": "must be an email"},
            "full_name": {"bsonType": ["string", "null"]},
            "created_at": {"bsonType": "date"},
            "last_login_time": {"bsonType": ["date", "null"]},
            "is_active": {"bsonType": "bool"},
            "metadata": {"bsonType": ["object", "null"]}
        }
    }
}


SAMPLE_DOCS = [
    {
        "user_id": "u_001",
        "username": "lakshmi",
        "email": "lakshmi@ucd.com",
        "full_name": "Lakshmi E.",
        "created_at": datetime(2024, 11, 14, 9, 0, 0),
        "last_login_time": datetime(2025, 10, 26, 20, 0, 0),
        "is_active": True,
        "metadata": {"country": "IE", "signup_source": "web"}
    },
    {
        "user_id": "u_002",
        "username": "razik",
        "email": "razik@ucd.com",
        "full_name": "Razik Shariff",
        "created_at": datetime(2025, 1, 10, 14, 20, 0),
        "last_login_time": None,
        "is_active": True,
        "metadata": {"country": "IN", "signup_source": "mobile"}
    },
    {
        "user_id": "u_003",
        "username": "abby",
        "email": "abby@ucd.com",
        "full_name": "Abhishek Nair",
        "created_at": datetime(2025, 5, 2, 8, 30, 0),
        "last_login_time": datetime(2025, 10, 25, 12, 15, 0),
        "is_active": True,
        "metadata": {"country": "IE", "signup_source": "referral"}
    }
]


def ensure_collection(db):
    coll_names = db.list_collection_names()
    if COLLECTION_NAME in coll_names:
        print(f"Collection '{COLLECTION_NAME}' already exists. Updating validator if needed.")
        try:
            db.command({
                'collMod': COLLECTION_NAME,
                'validator': VALIDATOR,
                'validationLevel': 'moderate'
            })
            print("Validator applied via collMod.")
        except errors.OperationFailure as e:
            print("Could not modify collection validator:", e)
    else:
        print(f"Creating collection '{COLLECTION_NAME}' with JSON schema validator...")
        try:
            db.create_collection(COLLECTION_NAME, validator=VALIDATOR, validationLevel='moderate')
            print("Collection created.")
        except errors.CollectionInvalid as e:
            print("Collection creation failed:", e)


def create_indexes(coll):
    print("Creating indexes...")
    coll.create_index([('user_id', 1)], unique=True, name='uq_user_id')
    coll.create_index([('username', 1)], unique=True, name='uq_username')
    coll.create_index([('email', 1)], unique=True, name='uq_email')
    coll.create_index([('last_login_time', 1)], name='idx_last_login_time')
    print('Indexes created:')
    for idx in coll.index_information().items():
        print(idx)


def insert_sample_data(coll):
    print('Inserting sample documents (duplicates will be ignored if any)...')
    try:
        res = coll.insert_many(SAMPLE_DOCS, ordered=False)
        print(f'Inserted {len(res.inserted_ids)} documents.')
    except errors.BulkWriteError as bwe:
        # If some docs violate unique constraints, we still continue
        details = bwe.details
        inserted = details.get('nInserted', 0)
        print(f'Bulk write error (some documents may already exist). Inserted {inserted} docs.')
        print('Error details excerpt:')
        pprint({k: details.get(k) for k in ('writeErrors', 'writeConcernErrors')})
    except Exception as e:
        print('Insert failed:', e)


def preview_collection(coll, limit=10):
    print(f'Previewing up to {limit} documents from collection:')
    docs = list(coll.find().limit(limit))
    # Use bson.json_util to convert BSON (dates/ObjectId) to JSON-friendly strings
    print(json_util.dumps(docs, indent=2))


def main():
    parser = argparse.ArgumentParser(description='Create user_profiles collection and insert sample data.')
    parser.add_argument('--uri', default=DEFAULT_URI, help='MongoDB connection URI (default: mongodb://localhost:27017)')
    parser.add_argument('--db', default=DEFAULT_DB, help='Database name (default: mydb)')
    args = parser.parse_args()

    print('Connecting to MongoDB at', args.uri)
    client = MongoClient(args.uri)
    db = client[args.db]

    ensure_collection(db)
    coll = db[COLLECTION_NAME]
    create_indexes(coll)
    insert_sample_data(coll)
    preview_collection(coll)

    print('Done.')

if __name__ == '__main__':
    main()