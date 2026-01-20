import sys
import argparse
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
sys.path.append(str(Path(__file__).parent))

from config import get_minio_client


def run_pipeline(generate_data: bool = False, skip_mongodb: bool = False):
    try:
        get_minio_client().list_buckets()
    except Exception as e:
        print(f"Erreur MinIO: {e}\nLancez: docker compose up -d")
        sys.exit(1)

    if generate_data:
        from generate import generate_clients, generate_achats
        base_dir = Path(__file__).parent.parent / "data"
        client_ids = generate_clients(1500, str(base_dir / "clients.csv"))
        generate_achats(client_ids, 5000, str(base_dir / "achats.csv"))

    from bronze import upload_data_to_bronze
    upload_data_to_bronze()

    from silver import transform_clients_to_silver, transform_achats_to_silver
    transform_clients_to_silver()
    transform_achats_to_silver()

    from gold import transform_to_gold
    transform_to_gold()

    if not skip_mongodb:
        try:
            from mongodb_sync import transform_gold_to_mongodb
            transform_gold_to_mongodb()
        except Exception as e:
            print(f"⚠ MongoDB sync skipped: {e}")
            print("Make sure MongoDB is running: docker compose up -d")

    print("\n" + "="*60)
    print("✅ Pipeline terminé avec succès!")
    print("="*60)
    print("\n📊 Accès aux services:")
    print("   • MinIO:    http://localhost:9001 (minioadmin/minioadmin)")
    print("   • Prefect:  http://localhost:4200")
    print("   • MongoDB:  mongodb://admin:admin123@localhost:27017")
    print("   • API:      http://localhost:5000")
    print("   • Dashboard: streamlit run dashboard/streamlit_app.py")
    print("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--skip-mongodb", action="store_true")
    args = parser.parse_args()
    run_pipeline(args.generate, args.skip_mongodb)

