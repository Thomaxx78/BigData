import sys
from pathlib import Path
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from datetime import datetime, timedelta
import os

sys.path.append(str(Path(__file__).parent.parent))
from config import (
    get_mongodb_client, get_mongodb_database,
    COLLECTION_CLIENTS, COLLECTION_ACHATS, COLLECTION_KPI, COLLECTION_SYNC_LOG
)

app = Flask(__name__, static_folder='dashboard', static_url_path='')
CORS(app)

# MongoDB connection
try:
    mongodb_client = get_mongodb_client()
    db = get_mongodb_database(mongodb_client)
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")
    db = None


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    try:
        if db:
            db.admin.command('ping')
            return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()}), 200
    except:
        pass
    return jsonify({"status": "unhealthy"}), 503


@app.route('/api/clients', methods=['GET'])
def get_clients():
    """Get all clients with optional filters."""
    try:
        page = int(request.args.get('page', 0))
        limit = int(request.args.get('limit', 50))
        pays = request.args.get('pays')
        
        query = {}
        if pays:
            query['pays'] = pays
        
        total = db[COLLECTION_CLIENTS].count_documents(query)
        clients = list(db[COLLECTION_CLIENTS].find(query).skip(page * limit).limit(limit))
        
        # Convert ObjectId to string
        for client in clients:
            client['_id'] = str(client['_id'])
        
        return jsonify({
            "total": total,
            "page": page,
            "limit": limit,
            "data": clients
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/clients/<int:client_id>', methods=['GET'])
def get_client_detail(client_id):
    """Get client details by ID."""
    try:
        client = db[COLLECTION_CLIENTS].find_one({"client_id": client_id})
        if not client:
            return jsonify({"error": "Client not found"}), 404
        
        client['_id'] = str(client['_id'])
        
        # Get client purchases
        purchases = list(db[COLLECTION_ACHATS].find({"client_id": client_id}))
        for purchase in purchases:
            purchase['_id'] = str(purchase['_id'])
        
        client['purchases'] = purchases
        return jsonify(client), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/purchases', methods=['GET'])
def get_purchases():
    """Get purchases with optional filters."""
    try:
        page = int(request.args.get('page', 0))
        limit = int(request.args.get('limit', 50))
        statut = request.args.get('statut')
        min_amount = request.args.get('min_amount', type=float)
        max_amount = request.args.get('max_amount', type=float)
        
        query = {}
        if statut:
            query['statut'] = statut
        if min_amount:
            query['montant_total'] = {'$gte': min_amount}
        if max_amount:
            if 'montant_total' in query:
                query['montant_total']['$lte'] = max_amount
            else:
                query['montant_total'] = {'$lte': max_amount}
        
        total = db[COLLECTION_ACHATS].count_documents(query)
        purchases = list(db[COLLECTION_ACHATS].find(query).skip(page * limit).limit(limit))
        
        for purchase in purchases:
            purchase['_id'] = str(purchase['_id'])
        
        return jsonify({
            "total": total,
            "page": page,
            "limit": limit,
            "data": purchases
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/kpi', methods=['GET'])
def get_kpi():
    """Get current KPI - calculated from live data."""
    try:
        # Count collections
        total_clients = db[COLLECTION_CLIENTS].count_documents({})
        total_achats = db[COLLECTION_ACHATS].count_documents({})
        
        # Calculate from purchases
        all_purchases = list(db[COLLECTION_ACHATS].find({}))
        delivered = [p for p in all_purchases if p.get('statut') == 'livré']
        cancelled = [p for p in all_purchases if p.get('statut') == 'annulé']
        
        ca_total = sum(p.get('montant_total', 0) for p in delivered)
        panier_moyen = ca_total / len(delivered) if delivered else 0
        taux_annulation = (len(cancelled) / total_achats * 100) if total_achats > 0 else 0
        
        kpi_data = {
            "total_clients": total_clients,
            "total_achats": total_achats,
            "ca_total": round(ca_total, 2),
            "panier_moyen": round(panier_moyen, 2),
            "taux_annulation": round(taux_annulation, 2),
            "date_update": datetime.utcnow().isoformat(),
            "calculated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return jsonify(kpi_data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """Get various statistics."""
    try:
        # Count by status
        status_stats = list(db[COLLECTION_ACHATS].aggregate([
            {"$group": {"_id": "$statut", "count": {"$sum": 1}, "total_amount": {"$sum": "$montant_total"}}}
        ]))
        
        # Count by country
        country_stats = list(db[COLLECTION_CLIENTS].aggregate([
            {"$group": {"_id": "$pays", "count": {"$sum": 1}}}
        ]))
        
        # Count by category
        category_stats = list(db[COLLECTION_ACHATS].aggregate([
            {"$group": {"_id": "$categorie", "count": {"$sum": 1}, "total_amount": {"$sum": "$montant_total"}}}
        ]))
        
        return jsonify({
            "by_status": status_stats,
            "by_country": country_stats,
            "by_category": category_stats
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/sync-log', methods=['GET'])
def get_sync_log():
    """Get synchronization logs."""
    try:
        days = int(request.args.get('days', 7))
        since = datetime.utcnow() - timedelta(days=days)
        
        logs = list(db[COLLECTION_SYNC_LOG].find(
            {"timestamp": {"$gte": since}}
        ).sort("timestamp", -1).limit(100))
        
        for log in logs:
            log['_id'] = str(log['_id'])
            log['timestamp'] = log['timestamp'].isoformat() if isinstance(log['timestamp'], datetime) else str(log['timestamp'])
        
        return jsonify({"data": logs}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get overall system status."""
    try:
        clients_count = db[COLLECTION_CLIENTS].count_documents({})
        purchases_count = db[COLLECTION_ACHATS].count_documents({})
        kpi_count = db[COLLECTION_KPI].count_documents({})
        
        # Get latest sync time
        latest_sync = db[COLLECTION_SYNC_LOG].find_one({}, sort=[('timestamp', -1)])
        
        return jsonify({
            "clients": clients_count,
            "purchases": purchases_count,
            "kpi": kpi_count,
            "last_sync": latest_sync['timestamp'].isoformat() if latest_sync and isinstance(latest_sync.get('timestamp'), datetime) else None,
            "database": db.name
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/')
def index():
    """Serve dashboard."""
    return send_from_directory('dashboard', 'index.html')


@app.errorhandler(404)
def not_found(e):
    """Handle 404 errors."""
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def server_error(e):
    """Handle 500 errors."""
    return jsonify({"error": "Internal server error"}), 500


if __name__ == '__main__':
    print("🚀 Starting Flask API on http://localhost:5000")
    print("📊 API Documentation: http://localhost:5000/api/status")
    print("🎯 Dashboard: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)
