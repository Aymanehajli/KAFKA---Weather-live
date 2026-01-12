#!/usr/bin/env python3
"""
Chargeur de données météo historiques - Exercice 9
Télécharge des données sur 10 ans via l'API archive Open-Meteo et les stocke dans Kafka et HDFS.
"""

import argparse
import json
import sys
import os
import time
import subprocess
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


def get_coordinates_from_city(city, country=None):
    """
    Utilise l'API de géocodage Open-Meteo pour obtenir les coordonnées d'une ville.
    
    Args:
        city: Nom de la ville
        country: Nom du pays (optionnel)
    
    Returns:
        tuple: (latitude, longitude, location_name, city_name, country_name)
    """
    url = "https://geocoding-api.open-meteo.com/v1/search"
    params = {
        'name': city,
        'count': 1,
        'language': 'fr',
        'format': 'json'
    }
    
    if country:
        params['name'] = f"{city}, {country}"
    
    try:
        print(f"🔍 Recherche des coordonnées pour: {params['name']}...")
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if 'results' in data and len(data['results']) > 0:
            result = data['results'][0]
            latitude = result.get('latitude')
            longitude = result.get('longitude')
            name = result.get('name', city)
            country_name = result.get('country', country or '')
            admin1 = result.get('admin1', '')
            
            full_name = f"{name}, {admin1}, {country_name}".strip(', ')
            
            print(f"✅ Coordonnées trouvées: {latitude}, {longitude} ({full_name})")
            return latitude, longitude, full_name, name, country_name or country
        else:
            print(f"⚠️  Aucune localisation trouvée pour: {params['name']}", file=sys.stderr)
            return None, None, None, None, None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la requête de géocodage: {e}", file=sys.stderr)
        return None, None, None, None, None


def fetch_historical_weather(latitude, longitude, start_date, end_date):
    """
    Télécharge les données météo historiques depuis l'API archive Open-Meteo.
    
    Args:
        latitude: Latitude
        longitude: Longitude
        start_date: Date de début (YYYY-MM-DD)
        end_date: Date de fin (YYYY-MM-DD)
    
    Returns:
        dict: Données météo historiques ou None en cas d'erreur
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'start_date': start_date,
        'end_date': end_date,
        'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,windspeed_10m_max,windspeed_10m_mean,winddirection_10m_dominant',
        'timezone': 'auto'
    }
    
    try:
        print(f"📥 Téléchargement des données historiques: {start_date} à {end_date}...")
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        if 'daily' in data:
            print(f"✅ {len(data['daily']['time'])} jour(s) de données téléchargé(s)")
            return data
        else:
            print("⚠️  Aucune donnée quotidienne trouvée dans la réponse", file=sys.stderr)
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la requête API: {e}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Erreur lors du parsing JSON: {e}", file=sys.stderr)
        return None


def create_producer(bootstrap_servers='localhost:29092'):
    """Crée un producteur Kafka."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )
        return producer
    except KafkaError as e:
        print(f"❌ Erreur lors de la création du producteur: {e}", file=sys.stderr)
        sys.exit(1)


def send_to_kafka(producer, topic, data, location_key):
    """Envoie les données à Kafka."""
    try:
        future = producer.send(topic, key=location_key, value=data)
        record_metadata = future.get(timeout=10)
        return True
    except Exception as e:
        print(f"⚠️  Erreur lors de l'envoi à Kafka: {e}", file=sys.stderr)
        return False


def save_to_hdfs(data, country, city, hdfs_base_path="/hdfs-data"):
    """
    Sauvegarde les données dans HDFS.
    
    Args:
        data: Données à sauvegarder (dict)
        country: Nom du pays
        city: Nom de la ville
        hdfs_base_path: Chemin de base HDFS
    """
    try:
        # Nettoyer les noms
        country_clean = country.replace('/', '_').replace(' ', '_') if country else "Unknown"
        city_clean = city.replace('/', '_').replace(' ', '_') if city else "Unknown"
        
        # Construire le chemin HDFS
        hdfs_dir = f"{hdfs_base_path}/{country_clean}/{city_clean}/weather_history_raw"
        hdfs_file = f"{hdfs_dir}/historical_data.json"
        
        # Créer le répertoire
        subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', hdfs_dir],
            check=True,
            capture_output=True,
            timeout=10
        )
        
        # Écrire dans un fichier temporaire
        temp_file = f"/tmp/weather_history_{country_clean}_{city_clean}_{int(time.time())}.json"
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        # Copier dans le conteneur
        subprocess.run(
            ['docker', 'cp', temp_file, f'namenode:/tmp/weather_history_temp.json'],
            check=True,
            capture_output=True,
            timeout=10
        )
        
        # Déplacer dans HDFS
        subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', '-f', '/tmp/weather_history_temp.json', hdfs_file],
            check=True,
            capture_output=True,
            timeout=10
        )
        
        # Nettoyer
        os.remove(temp_file)
        subprocess.run(
            ['docker', 'exec', 'namenode', 'rm', '-f', '/tmp/weather_history_temp.json'],
            capture_output=True
        )
        
        print(f"✅ Données sauvegardées dans HDFS: {hdfs_file}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de la sauvegarde HDFS: {e}", file=sys.stderr)
        if e.stderr:
            print(f"   stderr: {e.stderr.decode()}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}", file=sys.stderr)
        return False


def main():
    """Fonction principale."""
    parser = argparse.ArgumentParser(
        description='Chargeur de données météo historiques - Télécharge 10 ans de données et les stocke dans Kafka et HDFS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  %(prog)s --city Paris --country France
  %(prog)s --city "New York" --country USA --years 5
  %(prog)s --lat 48.8566 --lon 2.3522 --city Paris --country France
        """
    )
    
    # Arguments de localisation
    coord_group = parser.add_argument_group('Coordonnées')
    coord_group.add_argument('--lat', '--latitude', type=float, dest='latitude', help='Latitude')
    coord_group.add_argument('--lon', '--longitude', type=float, dest='longitude', help='Longitude')
    
    city_group = parser.add_argument_group('Ville et pays')
    city_group.add_argument('--city', type=str, help='Nom de la ville')
    city_group.add_argument('--country', type=str, help='Nom du pays')
    
    # Arguments de configuration
    parser.add_argument('--years', type=int, default=10, help='Nombre d\'années de données (défaut: 10)')
    parser.add_argument('--kafka-topic', default='weather_history', help='Topic Kafka (défaut: weather_history)')
    parser.add_argument('--bootstrap-servers', default='localhost:29092', help='Serveur Kafka (défaut: localhost:29092)')
    parser.add_argument('--hdfs-path', default='/hdfs-data', help='Chemin HDFS de base (défaut: /hdfs-data)')
    parser.add_argument('--skip-kafka', action='store_true', help='Ne pas envoyer à Kafka, seulement HDFS')
    parser.add_argument('--skip-hdfs', action='store_true', help='Ne pas sauvegarder dans HDFS, seulement Kafka')
    
    args = parser.parse_args()
    
    # Déterminer les coordonnées
    latitude = None
    longitude = None
    city = None
    country = None
    location_name = None
    
    if args.city:
        lat, lon, loc_name, city_name, country_name = get_coordinates_from_city(args.city, args.country)
        if lat is None or lon is None:
            print("❌ Erreur: Impossible d'obtenir les coordonnées", file=sys.stderr)
            sys.exit(1)
        latitude = lat
        longitude = lon
        city = city_name
        country = country_name
        location_name = loc_name
    elif args.latitude is not None and args.longitude is not None:
        latitude = args.latitude
        longitude = args.longitude
        city = args.city or "Unknown"
        country = args.country or "Unknown"
    else:
        print("❌ Erreur: Vous devez spécifier soit --city (et optionnellement --country), soit --lat et --lon", file=sys.stderr)
        parser.print_help()
        sys.exit(1)
    
    # Calculer les dates (10 ans en arrière depuis aujourd'hui)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.years * 365)
    
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print("=" * 60)
    print("📊 Chargement de données météo historiques")
    print("=" * 60)
    print(f"📍 Localisation: {city}, {country}")
    print(f"📅 Période: {start_date_str} à {end_date_str} ({args.years} ans)")
    print("=" * 60)
    print()
    
    # Télécharger les données
    historical_data = fetch_historical_weather(latitude, longitude, start_date_str, end_date_str)
    
    if not historical_data:
        print("❌ Impossible de télécharger les données historiques", file=sys.stderr)
        sys.exit(1)
    
    # Enrichir les données avec les métadonnées
    enriched_data = {
        'metadata': {
            'location': {
                'latitude': latitude,
                'longitude': longitude,
                'city': city,
                'country': country,
                'location_name': location_name
            },
            'period': {
                'start_date': start_date_str,
                'end_date': end_date_str,
                'years': args.years
            },
            'downloaded_at': datetime.now().isoformat(),
            'source': 'open-meteo-archive'
        },
        'data': historical_data
    }
    
    # Envoyer à Kafka
    if not args.skip_kafka:
        print("\n📤 Envoi des données à Kafka...")
        producer = create_producer(args.bootstrap_servers)
        location_key = f"{country}/{city}" if city != "Unknown" else f"{latitude},{longitude}"
        
        if send_to_kafka(producer, args.kafka_topic, enriched_data, location_key):
            print(f"✅ Données envoyées à Kafka: topic {args.kafka_topic}")
        else:
            print("⚠️  Échec de l'envoi à Kafka", file=sys.stderr)
        
        producer.close()
    
    # Sauvegarder dans HDFS
    if not args.skip_hdfs:
        print("\n💾 Sauvegarde dans HDFS...")
        if save_to_hdfs(enriched_data, country, city, args.hdfs_path):
            print("✅ Données sauvegardées dans HDFS")
        else:
            print("⚠️  Échec de la sauvegarde HDFS", file=sys.stderr)
    
    print("\n" + "=" * 60)
    print("✅ Chargement terminé!")
    print("=" * 60)
    
    # Afficher un résumé
    if 'daily' in historical_data:
        daily_data = historical_data['daily']
        print(f"\n📊 Résumé des données:")
        print(f"   Nombre de jours: {len(daily_data.get('time', []))}")
        if 'temperature_2m_mean' in daily_data:
            temps = [t for t in daily_data['temperature_2m_mean'] if t is not None]
            if temps:
                print(f"   Température moyenne: {min(temps):.1f}°C à {max(temps):.1f}°C")
        if 'precipitation_sum' in daily_data:
            precips = [p for p in daily_data['precipitation_sum'] if p is not None]
            if precips:
                print(f"   Précipitations totales: {sum(precips):.1f} mm")


if __name__ == '__main__':
    main()
