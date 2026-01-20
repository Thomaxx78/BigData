import pandas as pd
from pathlib import Path
from io import BytesIO
import sys

sys.path.append(str(Path(__file__).parent.parent))
from config import get_minio_client, BUCKET_SILVER, BUCKET_GOLD


def load_from_minio(bucket: str, object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    parquet_name = object_name.replace(".csv", ".parquet")
    try:
        response = client.get_object(bucket, parquet_name)
        df = pd.read_parquet(BytesIO(response.read()))
    except Exception:
        response = client.get_object(bucket, object_name)
        df = pd.read_csv(BytesIO(response.read()))
    response.close()
    response.release_conn()
    return df


def save_to_minio(df: pd.DataFrame, object_name: str) -> None:
    client = get_minio_client()
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    client.put_object(BUCKET_GOLD, object_name, csv_buffer, length=len(csv_buffer.getvalue()), content_type="text/csv")


def transform_to_gold():
    df_clients = load_from_minio(BUCKET_SILVER, "clients_silver.csv")
    df_achats = load_from_minio(BUCKET_SILVER, "achats_silver.csv")

    dim_pays = df_clients[["pays"]].drop_duplicates().reset_index(drop=True)
    dim_pays["pays_id"] = dim_pays.index + 1
    regions = {"USA": "Amérique du Nord", "Canada": "Amérique du Nord", "UK": "Europe",
               "Germany": "Europe", "France": "Europe", "Australia": "Océanie",
               "India": "Asie", "Brazil": "Amérique du Sud", "Japan": "Asie", "China": "Asie"}
    dim_pays["region"] = dim_pays["pays"].map(regions)
    save_to_minio(dim_pays, "dim_pays.csv")

    dim_produits = df_achats[["produit", "categorie"]].drop_duplicates().reset_index(drop=True)
    dim_produits["produit_id"] = dim_produits.index + 1
    save_to_minio(dim_produits, "dim_produits.csv")

    dim_clients = df_clients.merge(dim_pays[["pays_id", "pays"]], on="pays", how="left")
    save_to_minio(dim_clients, "dim_clients.csv")

    fact_achats = df_achats.merge(dim_produits[["produit_id", "produit"]], on="produit", how="left")
    fact_achats = fact_achats.merge(dim_clients[["client_id", "pays_id"]], on="client_id", how="left")
    save_to_minio(fact_achats, "fact_achats.csv")

    df_livres = df_achats[df_achats["statut"] == "livré"]

    kpi_global = pd.DataFrame([{
        "total_clients": len(df_clients),
        "total_achats": len(df_achats),
        "ca_total": round(df_livres["montant_total"].sum(), 2),
        "panier_moyen": round(df_livres["montant_total"].mean(), 2),
        "taux_annulation": round(len(df_achats[df_achats["statut"] == "annulé"]) / len(df_achats) * 100, 2)
    }])
    save_to_minio(kpi_global, "kpi_global.csv")

    df_avec_pays = df_livres.merge(df_clients[["client_id", "pays"]], on="client_id")
    ca_pays = df_avec_pays.groupby("pays").agg(
        ca_total=("montant_total", "sum"), nb_achats=("achat_id", "count")
    ).reset_index().sort_values("ca_total", ascending=False)
    ca_pays["ca_total"] = ca_pays["ca_total"].round(2)
    save_to_minio(ca_pays, "ca_par_pays.csv")

    ca_categorie = df_livres.groupby("categorie").agg(
        ca_total=("montant_total", "sum"), nb_achats=("achat_id", "count")
    ).reset_index().sort_values("ca_total", ascending=False)
    ca_categorie["ca_total"] = ca_categorie["ca_total"].round(2)
    save_to_minio(ca_categorie, "ca_par_categorie.csv")

    df_livres = df_livres.copy()
    df_livres["date"] = pd.to_datetime(df_livres["date_achat"])

    agg_jour = df_livres.groupby(df_livres["date"].dt.date).agg(
        ca=("montant_total", "sum"), nb_achats=("achat_id", "count")
    ).reset_index()
    agg_jour.columns = ["date", "ca", "nb_achats"]
    save_to_minio(agg_jour, "agg_par_jour.csv")

    df_livres["mois"] = df_livres["date"].dt.to_period("M").astype(str)
    agg_mois = df_livres.groupby("mois").agg(
        ca=("montant_total", "sum"), nb_achats=("achat_id", "count")
    ).reset_index()
    agg_mois["croissance_pct"] = (agg_mois["ca"].pct_change() * 100).round(2)
    save_to_minio(agg_mois, "agg_par_mois.csv")

    agg_annee = df_livres.groupby(df_livres["date"].dt.year).agg(
        ca=("montant_total", "sum"), nb_achats=("achat_id", "count")
    ).reset_index()
    agg_annee.columns = ["annee", "ca", "nb_achats"]
    save_to_minio(agg_annee, "agg_par_annee.csv")

    dist_statut = df_achats["statut"].value_counts().reset_index()
    dist_statut.columns = ["statut", "count"]
    save_to_minio(dist_statut, "distribution_statut.csv")

    dist_paiement = df_achats["mode_paiement"].value_counts().reset_index()
    dist_paiement.columns = ["mode_paiement", "count"]
    save_to_minio(dist_paiement, "distribution_paiement.csv")

    print(f"Gold: {len(list(get_minio_client().list_objects(BUCKET_GOLD)))} fichiers")


if __name__ == "__main__":
    transform_to_gold()
