import sys
from pathlib import Path
import pandas as pd
from io import BytesIO
import time
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))
from config import (
    get_minio_client, BUCKET_GOLD,
    get_mongodb_database, get_mongodb_client, create_indexes,
    COLLECTION_CLIENTS, COLLECTION_ACHATS, COLLECTION_KPI,
    clear_collection, log_sync
)


def load_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    parquet_name = object_name.replace(".csv", ".parquet")
    try:
        response = client.get_object(bucket, parquet_name)
        df = pd.read_parquet(BytesIO(response.read()))
    except Exception:
        try:
            response = client.get_object(bucket, object_name)
            df = pd.read_csv(BytesIO(response.read()))
        except Exception as e:
            print(f"✗ Error loading {object_name}: {e}")
            return pd.DataFrame()
    finally:
        if response:
            response.close()
            response.release_conn()
    return df


def load_to_mongodb(db, collection_name: str, df: pd.DataFrame) -> dict:
    if df.empty:
        print(f"⚠ DataFrame is empty for collection: {collection_name}")
        return {"status": "skipped", "row_count": 0, "duration_seconds": 0}
    
    start_time = time.time()
    deleted_count = clear_collection(db, collection_name)
    records = df.to_dict(orient='records')

    if records:
        result = db[collection_name].insert_many(records)
        duration = time.time() - start_time
        log_entry = log_sync(db, collection_name, "success", len(result.inserted_ids), duration)
        
        print(f"✓ {collection_name}: {len(result.inserted_ids)} documents loaded in {duration:.2f}s")
        return log_entry
    else:
        return {"status": "empty", "row_count": 0, "duration_seconds": 0}


def transform_gold_to_mongodb():
    print("\n" + "="*60)
    print("🔄 Starting Gold → MongoDB Pipeline")
    print("="*60)
    
    try:
        mdb_client = get_mongodb_client()
        db = get_mongodb_database(mdb_client)
        create_indexes(db)

        print("\n📥 Loading Clients dimension...")
        df_clients = load_from_minio(BUCKET_GOLD, "dim_clients.csv")
        log_clients = load_to_mongodb(db, COLLECTION_CLIENTS, df_clients)

        print("\n📥 Loading Purchases fact table...")
        df_achats = load_from_minio(BUCKET_GOLD, "fact_achats.csv")
        log_achats = load_to_mongodb(db, COLLECTION_ACHATS, df_achats)

        print("\n📥 Loading KPI...")
        df_kpi = load_from_minio(BUCKET_GOLD, "kpi_global.csv")
        if not df_kpi.empty:
            df_kpi['date_update'] = datetime.utcnow()
            log_kpi = load_to_mongodb(db, COLLECTION_KPI, df_kpi)
        
        print("\n" + "="*60)
        print("✅ Gold → MongoDB Pipeline Completed Successfully")
        print("="*60)

        total_clients = db[COLLECTION_CLIENTS].count_documents({})
        total_achats = db[COLLECTION_ACHATS].count_documents({})
        
        print(f"\n📊 Summary:")
        print(f"   Clients: {total_clients}")
        print(f"   Purchases: {total_achats}")
        print(f"   MongoDB: {db.name}")
        
        mdb_client.close()
        return True
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = transform_gold_to_mongodb()
    sys.exit(0 if success else 1)
