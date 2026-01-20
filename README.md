# MongoDB Analytics Platform

Plateforme d'analytics avec pipeline data lake (Bronze/Silver/Gold), MongoDB, API REST et dashboard.

## Architecture

```
CSV (data/)
    |
[Bronze] --> MinIO (raw)
    |
[Silver] --> MinIO (cleaned)
    |
[Gold] --> MinIO (aggregated, Parquet)
    |
[MongoDB Sync] --> MongoDB (operational)
    |
    +---> Flask API --> Streamlit Dashboard
    |
    +---> Metabase (BI)
```

## Prérequis

- Docker & Docker Compose
- Python 3.11+
- pip

## Installation

```bash
git clone <repo-url>
cd <repo-name>
pip install -r requirements.txt
```

## Lancement

### 1. Démarrer les services

```bash
docker compose up -d
```

Services lancés :
- MinIO (stockage objets) : http://localhost:9001
- MongoDB : localhost:27017
- Prefect (orchestration) : http://localhost:4200
- Metabase (BI) : http://localhost:3000

### 2. Exécuter le pipeline

```bash
python pipeline/run.py --generate
```

Options :
- `--generate` : génère des données de test (1500 clients, 5000 achats)
- `--skip-mongodb` : skip la synchronisation MongoDB

### 3. Lancer l'API

```bash
python api/app.py
```

API disponible sur http://localhost:5000

### 4. Lancer le dashboard

```bash
streamlit run dashboard/streamlit_app.py
```

Dashboard disponible sur http://localhost:8501

## Structure

```
.
├── config/
│   ├── __init__.py
│   ├── minio.py
│   └── mongodb.py
│
├── pipeline/
│   ├── run.py           # Orchestrateur
│   ├── generate.py      # Génération données
│   ├── bronze.py        # Upload MinIO
│   ├── silver.py        # Nettoyage
│   ├── gold.py          # Agrégations
│   └── mongodb_sync.py  # Sync MongoDB
│
├── api/
│   └── app.py           # API Flask
│
├── dashboard/
│   └── streamlit_app.py
│
├── data/                # Données générées
├── docker-compose.yml
├── requirements.txt
└── .env
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| GET /api/health | Health check |
| GET /api/status | Statut système |
| GET /api/clients | Liste clients (paginée) |
| GET /api/clients/:id | Détail client + achats |
| GET /api/purchases | Liste achats (filtrée) |
| GET /api/kpi | KPIs globaux |
| GET /api/statistics | Stats agrégées |
| GET /api/sync-log | Historique syncs |

## Credentials

| Service | URL | User | Password |
|---------|-----|------|----------|
| MinIO | http://localhost:9001 | minioadmin | minioadmin |
| MongoDB | localhost:27017 | admin | admin123 |
| Metabase | http://localhost:3000 | - | (setup au 1er login) |

## Collections MongoDB

**clients** : données clients avec pays, email, date inscription

**achats** : transactions avec produit, catégorie, montant, statut

**kpi** : métriques calculées (CA, panier moyen, taux annulation)

**sync_log** : logs de synchronisation avec durée et throughput

## Troubleshooting

```bash
# Vérifier les logs
docker compose logs -f

# Tester l'API
curl http://localhost:5000/api/health

# Redémarrer MongoDB
docker compose restart mongodb

# Relancer le pipeline
python pipeline/run.py
```
# BigData
