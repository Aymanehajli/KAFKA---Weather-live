#!/usr/bin/env python3
"""
API Backend pour le dashboard météo global
Sert les données depuis Kafka, HDFS et calcule les statistiques
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import json
import subprocess
import os
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
import threading
import time
from collections import deque
import glob

app = Flask(__name__, static_folder='dashboard/static', static_url_path='/static')
CORS(app)

# Configuration
KAFKA_SERVERS = os.getenv('KAFKA_SERVERS', 'localhost:9092')
HDFS_BASE_PATH = '/hdfs-data'
MAX_CACHE_SIZE = 1000

# Cache pour les données temps réel
realtime_cache = {
    'weather_transformed': deque(maxlen=MAX_CACHE_SIZE),
    'weather_anomalies': deque(maxlen=MAX_CACHE_SIZE),
    'weather_aggregates': deque(maxlen=MAX_CACHE_SIZE)
}

# Thread pour consommer Kafka en arrière-plan
kafka_consumer_thread = None
kafka_running = False


def consume_kafka_messages():
    """Consomme les messages Kafka en arrière-plan et les met en cache."""
    global kafka_running
    
    while True:
        try:
            consumer = KafkaConsumer(
                'weather_transformed',
                'weather_anomalies',
                'weather_aggregates',
                bootstrap_servers=KAFKA_SERVERS.split(','),
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
                consumer_timeout_ms=1000
            )
            
            kafka_running = True
            print("✅ Consommateur Kafka démarré")
            
            for message in consumer:
                topic = message.topic
                value = message.value
                
                if value and topic in realtime_cache:
                    value['_kafka_timestamp'] = datetime.now().isoformat()
                    realtime_cache[topic].append(value)
        
        except Exception as e:
            print(f"⚠️  Erreur dans le consommateur Kafka: {e}")
            kafka_running = False
            time.sleep(5)  # Réessayer après 5 secondes


def read_hdfs_file(hdfs_path):
    """Lit un fichier depuis HDFS."""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-cat', hdfs_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            return json.loads(result.stdout)
        return None
    except Exception as e:
        print(f"⚠️  Erreur lecture HDFS {hdfs_path}: {e}")
        return None


def list_hdfs_directory(hdfs_path):
    """Liste le contenu d'un répertoire HDFS."""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', hdfs_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            files = []
            for line in result.stdout.strip().split('\n'):
                if line and not line.startswith('Found'):
                    parts = line.split()
                    if len(parts) >= 8:
                        files.append({
                            'path': parts[-1],
                            'name': parts[-1].split('/')[-1],
                            'size': parts[4] if len(parts) > 4 else '0',
                            'type': 'directory' if parts[0].startswith('d') else 'file'
                        })
            return files
        return []
    except Exception as e:
        print(f"⚠️  Erreur liste HDFS {hdfs_path}: {e}")
        return []


@app.route('/')
def index():
    """Page principale du dashboard."""
    return send_from_directory('dashboard', 'index.html')


@app.route('/api/health')
def health():
    """Endpoint de santé."""
    return jsonify({
        'status': 'ok',
        'kafka_connected': kafka_running,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/realtime/weather')
def get_realtime_weather():
    """Récupère les dernières données météo en temps réel."""
    limit = request.args.get('limit', 100, type=int)
    
    data = list(realtime_cache['weather_transformed'])
    data.reverse()  # Plus récent en premier
    
    return jsonify({
        'data': data[:limit],
        'count': len(data),
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/realtime/anomalies')
def get_realtime_anomalies():
    """Récupère les dernières anomalies détectées."""
    limit = request.args.get('limit', 50, type=int)
    
    data = list(realtime_cache['weather_anomalies'])
    data.reverse()
    
    return jsonify({
        'data': data[:limit],
        'count': len(data),
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/realtime/aggregates')
def get_realtime_aggregates():
    """Récupère les agrégats en temps réel."""
    limit = request.args.get('limit', 100, type=int)
    
    data = list(realtime_cache['weather_aggregates'])
    data.reverse()
    
    return jsonify({
        'data': data[:limit],
        'count': len(data),
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/historical/<country>/<city>')
def get_historical_data(country, city):
    """Récupère les données historiques pour une ville."""
    hdfs_path = f"{HDFS_BASE_PATH}/{country}/{city}/weather_history_raw"
    
    # Chercher le fichier historique
    files = list_hdfs_directory(hdfs_path)
    historical_data = None
    
    for file in files:
        if file['type'] == 'file' and 'historical' in file['name']:
            data = read_hdfs_file(file['path'])
            if data:
                historical_data = data
                break
    
    return jsonify({
        'country': country,
        'city': city,
        'data': historical_data,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/alerts/<country>/<city>')
def get_alerts(country, city):
    """Récupère les alertes sauvegardées pour une ville."""
    hdfs_path = f"{HDFS_BASE_PATH}/{country}/{city}/alerts.json"
    
    alerts_data = read_hdfs_file(hdfs_path)
    
    return jsonify({
        'country': country,
        'city': city,
        'alerts': alerts_data.get('alerts', []) if alerts_data else [],
        'count': len(alerts_data.get('alerts', [])) if alerts_data else 0,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/records/<country>/<city>')
def get_records(country, city):
    """Récupère les records climatiques pour une ville."""
    hdfs_path = f"{HDFS_BASE_PATH}/{country}/{city}/weather_records/records.json"
    
    records_data = read_hdfs_file(hdfs_path)
    
    return jsonify({
        'country': country,
        'city': city,
        'records': records_data.get('records', []) if records_data else [],
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/seasonal-profile/<country>/<city>')
def get_seasonal_profile(country, city):
    """Récupère le profil saisonnier enrichi pour une ville."""
    # Chercher le profil enrichi le plus récent
    base_path = f"{HDFS_BASE_PATH}/{country}/{city}/seasonal_profile_enriched"
    
    # Lister les années disponibles
    years = []
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', base_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if line and not line.startswith('Found'):
                    parts = line.split()
                    if len(parts) >= 8:
                        year_path = parts[-1]
                        if year_path.split('/')[-1].isdigit():
                            years.append(int(year_path.split('/')[-1]))
    except:
        pass
    
    # Prendre l'année la plus récente
    if years:
        latest_year = max(years)
        profile_path = f"{base_path}/{latest_year}/profile.json"
        profile_data = read_hdfs_file(profile_path)
        
        return jsonify({
            'country': country,
            'city': city,
            'year': latest_year,
            'profile': profile_data,
            'timestamp': datetime.now().isoformat()
        })
    
    return jsonify({
        'country': country,
        'city': city,
        'profile': None,
        'error': 'No profile found'
    })


@app.route('/api/anomalies/<country>/<city>')
def get_anomalies(country, city):
    """Récupère les anomalies sauvegardées pour une ville."""
    base_path = f"{HDFS_BASE_PATH}/{country}/{city}/anomalies"
    
    all_anomalies = []
    
    # Parcourir les années et mois
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '-R', base_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if 'anomalies.json' in line:
                    parts = line.split()
                    if len(parts) >= 8:
                        file_path = parts[-1]
                        data = read_hdfs_file(file_path)
                        if data and 'anomalies' in data:
                            all_anomalies.extend(data['anomalies'])
    except Exception as e:
        print(f"⚠️  Erreur lecture anomalies: {e}")
    
    return jsonify({
        'country': country,
        'city': city,
        'anomalies': all_anomalies,
        'count': len(all_anomalies),
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/cities')
def get_cities():
    """Liste toutes les villes disponibles dans HDFS."""
    cities = []
    seen = set()  # Éviter les doublons
    
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', HDFS_BASE_PATH],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                if line and not line.startswith('Found') and not line.startswith('drwx'):
                    parts = line.split()
                    if len(parts) >= 8:
                        country_path = parts[-1]
                        if country_path.startswith(HDFS_BASE_PATH):
                            country = country_path.split('/')[-1]
                            
                            # Ignorer les fichiers à la racine
                            if '.' in country or not country:
                                continue
                            
                            # Lister les villes pour ce pays
                            try:
                                cities_result = subprocess.run(
                                    ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', country_path],
                                    capture_output=True,
                                    text=True,
                                    timeout=30
                                )
                                
                                if cities_result.returncode == 0:
                                    for city_line in cities_result.stdout.strip().split('\n'):
                                        if city_line and not city_line.startswith('Found') and not city_line.startswith('drwx'):
                                            city_parts = city_line.split()
                                            if len(city_parts) >= 8:
                                                city_path = city_parts[-1]
                                                city = city_path.split('/')[-1]
                                                
                                                # Ignorer les fichiers
                                                if '.' in city or not city:
                                                    continue
                                                
                                                key = f"{country}/{city}"
                                                if key not in seen:
                                                    seen.add(key)
                                                    cities.append({
                                                        'country': country,
                                                        'city': city
                                                    })
                            except:
                                continue
    except Exception as e:
        print(f"⚠️  Erreur liste villes: {e}")
    
    return jsonify({
        'cities': cities,
        'count': len(cities),
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/stats/summary')
def get_stats_summary():
    """Récupère un résumé des statistiques globales."""
    stats = {
        'realtime_messages': len(realtime_cache['weather_transformed']),
        'anomalies_detected': len(realtime_cache['weather_anomalies']),
        'aggregates_count': len(realtime_cache['weather_aggregates']),
        'kafka_connected': kafka_running,
        'timestamp': datetime.now().isoformat()
    }
    
    return jsonify(stats)


if __name__ == '__main__':
    # Démarrer le consommateur Kafka en arrière-plan
    kafka_consumer_thread = threading.Thread(target=consume_kafka_messages, daemon=True)
    kafka_consumer_thread.start()
    
    print("=" * 60)
    print("🌐 Dashboard API Météo")
    print("=" * 60)
    print(f"📡 Kafka: {KAFKA_SERVERS}")
    print(f"💾 HDFS: {HDFS_BASE_PATH}")
    print("=" * 60)
    print()
    print("🚀 Démarrage du serveur...")
    print("   API disponible sur: http://localhost:5001")
    print("   Dashboard disponible sur: http://localhost:5001")
    print()
    
    app.run(host='0.0.0.0', port=5001, debug=True, threaded=True)
