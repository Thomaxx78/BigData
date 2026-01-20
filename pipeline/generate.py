import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)


def generate_clients(n_clients: int, output_path: str) -> list[int]:
    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'India', 'Brazil', 'Japan', 'China']
    clients = []
    client_ids = []

    for i in range(1, n_clients + 1):
        date_inscription = fake.date_between(start_date='-3y', end_date='-1m')
        clients.append(
            {
                'client_id': i,
                'nom': fake.name(),
                'email': fake.email(),
                'date_inscription': date_inscription.strftime('%Y-%m-%d'),
                'pays': random.choice(countries)
            }
        )
        client_ids.append(i)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['client_id', 'nom', 'email', 'date_inscription', 'pays'])
        writer.writeheader()
        writer.writerows(clients)

    print(f"Généré {n_clients} clients -> {output_path}")
    return client_ids


def generate_achats(client_ids: list[int], n_achats: int, output_path: str) -> list[dict]:
    produits = [
        {"nom": "Laptop Pro", "categorie": "Électronique", "prix_base": 1299.99},
        {"nom": "Smartphone X", "categorie": "Électronique", "prix_base": 899.99},
        {"nom": "Tablette Air", "categorie": "Électronique", "prix_base": 599.99},
        {"nom": "Écouteurs Sans Fil", "categorie": "Électronique", "prix_base": 199.99},
        {"nom": "Montre Connectée", "categorie": "Électronique", "prix_base": 349.99},
        {"nom": "Clavier Mécanique", "categorie": "Accessoires", "prix_base": 129.99},
        {"nom": "Souris Gaming", "categorie": "Accessoires", "prix_base": 79.99},
        {"nom": "Webcam HD", "categorie": "Accessoires", "prix_base": 89.99},
        {"nom": "Disque SSD 1To", "categorie": "Stockage", "prix_base": 109.99},
        {"nom": "Disque SSD 2To", "categorie": "Stockage", "prix_base": 189.99},
        {"nom": "Câble USB-C", "categorie": "Accessoires", "prix_base": 19.99},
        {"nom": "Chargeur Rapide", "categorie": "Accessoires", "prix_base": 39.99},
        {"nom": "Coque Protection", "categorie": "Accessoires", "prix_base": 29.99},
        {"nom": "Support Laptop", "categorie": "Accessoires", "prix_base": 49.99},
        {"nom": "Hub USB", "categorie": "Accessoires", "prix_base": 59.99},
        {"nom": "Écran 27 pouces", "categorie": "Électronique", "prix_base": 399.99},
        {"nom": "Imprimante Laser", "categorie": "Électronique", "prix_base": 249.99},
        {"nom": "Routeur WiFi 6", "categorie": "Réseau", "prix_base": 149.99},
        {"nom": "Enceinte Bluetooth", "categorie": "Audio", "prix_base": 79.99},
        {"nom": "Casque Audio Pro", "categorie": "Audio", "prix_base": 299.99},
    ]

    statuts = ["livré", "livré", "livré", "livré", "en cours", "annulé"]
    modes_paiement = ["carte", "carte", "carte", "paypal", "virement"]

    achats = []

    for i in range(1, n_achats + 1):
        if random.random() < 0.3:
            client_id = random.choice(client_ids[:len(client_ids) // 10])
        else:
            client_id = random.choice(client_ids)

        produit = random.choice(produits)
        quantite = random.choices([1, 2, 3, 4, 5], weights=[70, 15, 8, 5, 2])[0]

        variation = random.uniform(0.9, 1.1)
        prix_unitaire = round(produit["prix_base"] * variation, 2)
        montant_total = round(prix_unitaire * quantite, 2)

        date_achat = fake.date_between(start_date="-2y", end_date="today")

        achats.append({
            "achat_id": i,
            "client_id": client_id,
            "produit": produit["nom"],
            "categorie": produit["categorie"],
            "quantite": quantite,
            "prix_unitaire": prix_unitaire,
            "montant_total": montant_total,
            "date_achat": date_achat.strftime("%Y-%m-%d"),
            "statut": random.choice(statuts),
            "mode_paiement": random.choice(modes_paiement),
        })

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "achat_id", "client_id", "produit", "categorie", "quantite",
            "prix_unitaire", "montant_total", "date_achat", "statut", "mode_paiement"
        ])
        writer.writeheader()
        writer.writerows(achats)

    total_ca = sum(a["montant_total"] for a in achats)
    print(f"Généré {n_achats} achats -> {output_path}")
    print(f"  - CA total: {total_ca:,.2f} €")
    print(f"  - Panier moyen: {total_ca / n_achats:.2f} €")

    return achats


if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data"
    client_ids = generate_clients(n_clients=1500, output_path=output_dir / "clients.csv")
    achats = generate_achats(client_ids=client_ids, n_achats=5000, output_path=output_dir / "achats.csv")
