from pathlib import Path
from io import BytesIO
import sys

sys.path.append(str(Path(__file__).parent.parent))
from config import get_minio_client, BUCKET_BRONZE, BUCKET_SOURCES


def upload_file_to_minio(local_path: str, object_name: str, bucket: str) -> None:
    client = get_minio_client()

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' créé")

    with open(local_path, "rb") as f:
        data = f.read()

    client.put_object(
        bucket,
        object_name,
        BytesIO(data),
        length=len(data),
        content_type="text/csv"
    )
    print(f"Uploadé: {local_path} -> {bucket}/{object_name} ({len(data)} bytes)")


def upload_data_to_bronze() -> None:
    print("=" * 60)
    print("UPLOAD DES DONNÉES VERS MINIO (Bronze)")
    print("=" * 60)

    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / "data"

    files_to_upload = [
        ("clients.csv", "clients.csv"),
        ("achats.csv", "achats.csv"),
    ]

    client = get_minio_client()
    print(f"\nConnexion MinIO OK")
    print(f"Buckets existants: {[b.name for b in client.list_buckets()]}")

    print(f"\nUpload vers bucket '{BUCKET_BRONZE}':")
    for local_name, remote_name in files_to_upload:
        local_path = data_dir / local_name
        if local_path.exists():
            upload_file_to_minio(str(local_path), remote_name, BUCKET_BRONZE)
        else:
            print(f"ATTENTION: Fichier non trouvé: {local_path}")

    print(f"\nContenu du bucket '{BUCKET_BRONZE}':")
    objects = list(client.list_objects(BUCKET_BRONZE))
    for obj in objects:
        print(f"  - {obj.object_name} ({obj.size} bytes)")

    print("\n" + "=" * 60)
    print("UPLOAD TERMINÉ")
    print("=" * 60)


if __name__ == "__main__":
    upload_data_to_bronze()
