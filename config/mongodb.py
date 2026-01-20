import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from datetime import datetime
import time

load_dotenv()

MONGODB_HOST = os.getenv("MONGODB_HOST", "localhost")
MONGODB_PORT = int(os.getenv("MONGODB_PORT", 27017))
MONGODB_USER = os.getenv("MONGODB_USER", "admin")
MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD", "admin123")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "analytics")

COLLECTION_CLIENTS = "clients"
COLLECTION_ACHATS = "achats"
COLLECTION_KPI = "kpi"
COLLECTION_SYNC_LOG = "sync_log"


def get_mongodb_client():
    connection_string = f"mongodb://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/{MONGODB_DATABASE}?authSource=admin"
    client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command('ping')
        print(f"✓ Connected to MongoDB at {MONGODB_HOST}:{MONGODB_PORT}")
    except Exception as e:
        print(f"✗ MongoDB connection error: {e}")
        raise
    return client


def get_mongodb_database(client=None):
    if client is None:
        client = get_mongodb_client()
    return client[MONGODB_DATABASE]


def create_indexes(db):
    try:
        db[COLLECTION_CLIENTS].create_index("client_id", unique=True)
        db[COLLECTION_CLIENTS].create_index("pays")
        db[COLLECTION_CLIENTS].create_index("date_inscription")

        db[COLLECTION_ACHATS].create_index("achat_id", unique=True)
        db[COLLECTION_ACHATS].create_index("client_id")
        db[COLLECTION_ACHATS].create_index("date_achat")
        db[COLLECTION_ACHATS].create_index("statut")
        db[COLLECTION_ACHATS].create_index("montant_total")

        db[COLLECTION_KPI].create_index("date_update")

        db[COLLECTION_SYNC_LOG].create_index("timestamp")
        
        print("✓ MongoDB indexes created")
    except Exception as e:
        print(f"✗ Index creation error: {e}")


def log_sync(db, collection_name, status, row_count, duration_seconds):
    log_entry = {
        "timestamp": datetime.utcnow(),
        "collection": collection_name,
        "status": status,
        "row_count": row_count,
        "duration_seconds": round(duration_seconds, 3),
        "documents_per_second": round(row_count / duration_seconds, 2) if duration_seconds > 0 else 0
    }
    db[COLLECTION_SYNC_LOG].insert_one(log_entry)
    return log_entry


def clear_collection(db, collection_name):
    result = db[collection_name].delete_many({})
    return result.deleted_count


if __name__ == "__main__":
    client = get_mongodb_client()
    db = get_mongodb_database(client)
    create_indexes(db)
    print(f"Database: {db.name}")
    print(f"Collections: {db.list_collection_names()}")
