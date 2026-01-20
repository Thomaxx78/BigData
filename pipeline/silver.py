import pandas as pd
from pathlib import Path
from io import BytesIO
import sys

sys.path.append(str(Path(__file__).parent.parent))
from config import get_minio_client, BUCKET_BRONZE, BUCKET_SILVER


def load_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(bucket, object_name)
    df = pd.read_csv(BytesIO(response.read()))
    response.close()
    response.release_conn()
    return df


def save_to_minio(df: pd.DataFrame, bucket: str, object_name: str) -> None:
    client = get_minio_client()
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    client.put_object(bucket, object_name, csv_buffer, length=len(csv_buffer.getvalue()), content_type="text/csv")
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    client.put_object(bucket, object_name.replace(".csv", ".parquet"), parquet_buffer, length=len(parquet_buffer.getvalue()))


def transform_clients_to_silver() -> pd.DataFrame:
    df = load_from_minio(BUCKET_BRONZE, "clients.csv")
    df = df.dropna(subset=["client_id"])
    df["nom"] = df["nom"].fillna("Inconnu")
    df["email"] = df["email"].fillna("non_renseigne@unknown.com")
    df["pays"] = df["pays"].fillna("Unknown")
    df = df[df["client_id"] > 0]
    df["date_inscription"] = pd.to_datetime(df["date_inscription"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["client_id"] = df["client_id"].astype(int)
    df["nom"] = df["nom"].str.strip().str.title()
    df["email"] = df["email"].str.strip().str.lower()
    df = df.drop_duplicates(subset=["client_id"], keep="first")
    df = df.drop_duplicates(subset=["email"], keep="first")
    save_to_minio(df, BUCKET_SILVER, "clients_silver.csv")
    print(f"Silver clients: {len(df)}")
    return df


def transform_achats_to_silver() -> pd.DataFrame:
    df = load_from_minio(BUCKET_BRONZE, "achats.csv")
    df = df.dropna(subset=["achat_id", "client_id", "montant_total"])
    df = df[df["montant_total"] > 0]
    df = df[df["quantite"] > 0]
    df["date_achat"] = pd.to_datetime(df["date_achat"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["achat_id"] = df["achat_id"].astype(int)
    df["client_id"] = df["client_id"].astype(int)
    df["statut"] = df["statut"].str.strip().str.lower()
    df["mode_paiement"] = df["mode_paiement"].str.strip().str.lower()
    df = df.drop_duplicates(subset=["achat_id"], keep="first")
    save_to_minio(df, BUCKET_SILVER, "achats_silver.csv")
    print(f"Silver achats: {len(df)}")
    return df


if __name__ == "__main__":
    transform_clients_to_silver()
    transform_achats_to_silver()
