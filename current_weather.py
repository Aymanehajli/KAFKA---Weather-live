#!/usr/bin/env python3
"""
Producteur Kafka pour les données météo en direct - Exercice 3 & 6
Interroge l'API Open-Meteo et envoie les données au topic weather_stream.
Supporte les coordonnées (lat/lon) ou ville/pays avec géocodage.
"""

import argparse
import json
import sys
import time
from datetime import datetime
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


def get_coordinates_from_city(city, country=None):
    """
    Utilise l'API de géocodage Open-Meteo pour obtenir les coordonnées d'une ville.
    
    Args:
        city: Nom de la ville
        country: Nom du pays (optionnel, pour plus de précision)
    
    Returns:
        tuple: (latitude, longitude, nom_complet) ou (None, None, None) en cas d'erreur
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
            admin1 = result.get('admin1', '')  # Région/État
            
            full_name = f"{name}, {admin1}, {country_name}".strip(', ')
            
            print(f"✅ Coordonnées trouvées: {latitude}, {longitude} ({full_name})")
            return latitude, longitude, full_name, name, country_name or country
        else:
            print(f"⚠️  Aucune localisation trouvée pour: {params['name']}", file=sys.stderr)
            return None, None, None, None, None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la requête de géocodage: {e}", file=sys.stderr)
        return None, None, None, None, None
    except (KeyError, json.JSONDecodeError) as e:
        print(f"❌ Erreur lors du parsing de la réponse de géocodage: {e}", file=sys.stderr)
        return None, None, None, None, None


def get_weather_data(latitude, longitude, city=None, country=None, location_name=None):
    """
    Interroge l'API Open-Meteo pour obtenir les données météo actuelles.
    
    Args:
        latitude: Latitude de la localisation
        longitude: Longitude de la localisation
    
    Returns:
        dict: Données météo au format JSON, ou None en cas d'erreur
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'current_weather': 'true',
        'timezone': 'auto'
    }
    
    try:
        print(f"🌤️  Interrogation de l'API Open-Meteo pour ({latitude}, {longitude})...")
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if 'current_weather' in data:
            # Enrichir les données avec des métadonnées
            location_data = {
                'latitude': latitude,
                'longitude': longitude
            }
            
            # Ajouter les informations de ville et pays si disponibles
            if city:
                location_data['city'] = city
            if country:
                location_data['country'] = country
            if location_name:
                location_data['location_name'] = location_name
            
            weather_data = {
                'timestamp': datetime.now().isoformat(),
                'location': location_data,
                'current_weather': data['current_weather'],
                'source': 'open-meteo',
                'api_response': data
            }
            return weather_data
        else:
            print("⚠️  Aucune donnée météo actuelle trouvée dans la réponse", file=sys.stderr)
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la requête API: {e}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Erreur lors du parsing JSON: {e}", file=sys.stderr)
        return None


def create_producer(bootstrap_servers='localhost:29092'):
    """
    Crée et configure un producteur Kafka.
    
    Args:
        bootstrap_servers: Adresse du serveur Kafka
    
    Returns:
        KafkaProducer configuré
    """
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


def send_weather_data(producer, topic, weather_data, location_key=None):
    """
    Envoie les données météo au topic Kafka.
    
    Args:
        producer: Instance KafkaProducer
        topic: Nom du topic Kafka
        weather_data: Données météo à envoyer
        location_key: Clé pour le message (optionnel, utilisée pour le partitionnement)
    """
    try:
        # Créer une clé basée sur ville/pays ou coordonnées pour le partitionnement
        if location_key is None:
            location = weather_data['location']
            if 'city' in location and 'country' in location:
                # Utiliser ville/pays pour le partitionnement HDFS
                location_key = f"{location['country']}/{location['city']}"
            else:
                # Fallback sur les coordonnées
                location_key = f"{location['latitude']},{location['longitude']}"
        
        # Envoyer le message
        future = producer.send(
            topic,
            key=location_key,
            value=weather_data
        )
        
        # Attendre la confirmation (optionnel, pour vérifier l'envoi)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Données météo envoyées avec succès!")
        print(f"   Topic: {record_metadata.topic}")
        print(f"   Partition: {record_metadata.partition}")
        print(f"   Offset: {record_metadata.offset}")
        print(f"   Température: {weather_data['current_weather'].get('temperature', 'N/A')}°C")
        print(f"   Conditions: {weather_data['current_weather'].get('weathercode', 'N/A')}")
        
        return True
        
    except KafkaError as e:
        print(f"❌ Erreur lors de l'envoi du message: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}", file=sys.stderr)
        return False


def main():
    """Fonction principale."""
    parser = argparse.ArgumentParser(
        description='Producteur météo Kafka - Interroge Open-Meteo et envoie les données au topic weather_stream',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  # Avec coordonnées:
  %(prog)s --lat 48.8566 --lon 2.3522
  %(prog)s --lat 48.8566 --lon 2.3522 --continuous
  
  # Avec ville et pays (géocodage automatique):
  %(prog)s --city Paris --country France
  %(prog)s --city "New York" --country USA --continuous
  %(prog)s --city London --country UK --interval 30
        """
    )
    
    # Groupe pour les coordonnées
    coord_group = parser.add_argument_group('Coordonnées')
    coord_group.add_argument(
        '--lat',
        '--latitude',
        type=float,
        dest='latitude',
        help='Latitude de la localisation'
    )
    
    coord_group.add_argument(
        '--lon',
        '--longitude',
        type=float,
        dest='longitude',
        help='Longitude de la localisation'
    )
    
    # Groupe pour ville/pays
    city_group = parser.add_argument_group('Ville et pays (géocodage)')
    city_group.add_argument(
        '--city',
        type=str,
        help='Nom de la ville'
    )
    
    city_group.add_argument(
        '--country',
        type=str,
        help='Nom du pays'
    )
    
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:29092',
        help='Adresse du serveur Kafka (défaut: localhost:29092)'
    )
    
    parser.add_argument(
        '--topic',
        default='weather_stream',
        help='Nom du topic Kafka (défaut: weather_stream)'
    )
    
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Mode continu: interroge l\'API périodiquement'
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Intervalle en secondes pour le mode continu (défaut: 60)'
    )
    
    args = parser.parse_args()
    
    # Déterminer les coordonnées et les informations de localisation
    latitude = None
    longitude = None
    city = None
    country = None
    location_name = None
    
    if args.city:
        # Mode géocodage: utiliser ville/pays
        if not args.country:
            print("⚠️  Avertissement: Le pays n'est pas spécifié, la recherche peut être moins précise", file=sys.stderr)
        
        lat, lon, loc_name, city_name, country_name = get_coordinates_from_city(args.city, args.country)
        
        if lat is None or lon is None:
            print("❌ Erreur: Impossible d'obtenir les coordonnées pour la ville spécifiée", file=sys.stderr)
            sys.exit(1)
        
        latitude = lat
        longitude = lon
        city = city_name
        country = country_name
        location_name = loc_name
        
    elif args.latitude is not None and args.longitude is not None:
        # Mode coordonnées directes
        latitude = args.latitude
        longitude = args.longitude
        
        # Valider les coordonnées
        if not (-90 <= latitude <= 90):
            print("❌ Erreur: La latitude doit être entre -90 et 90", file=sys.stderr)
            sys.exit(1)
        
        if not (-180 <= longitude <= 180):
            print("❌ Erreur: La longitude doit être entre -180 et 180", file=sys.stderr)
            sys.exit(1)
    else:
        print("❌ Erreur: Vous devez spécifier soit --city (et optionnellement --country), soit --lat et --lon", file=sys.stderr)
        parser.print_help()
        sys.exit(1)
    
    # Créer le producteur
    producer = create_producer(args.bootstrap_servers)
    
    try:
        if args.continuous:
            print(f"🔄 Mode continu activé (intervalle: {args.interval}s)")
            if city:
                print(f"📍 Localisation: {city}, {country}")
            print("Appuyez sur Ctrl+C pour arrêter\n")
            
            while True:
                # Récupérer les données météo
                weather_data = get_weather_data(latitude, longitude, city, country, location_name)
                
                if weather_data:
                    # Envoyer au topic Kafka
                    send_weather_data(producer, args.topic, weather_data)
                    print(f"⏳ Prochaine mise à jour dans {args.interval} secondes...\n")
                else:
                    print("⚠️  Échec de la récupération des données, nouvelle tentative dans 10s...\n")
                    time.sleep(10)
                    continue
                
                # Attendre avant la prochaine itération
                time.sleep(args.interval)
        else:
            # Mode unique: une seule requête
            weather_data = get_weather_data(latitude, longitude, city, country, location_name)
            
            if weather_data:
                send_weather_data(producer, args.topic, weather_data)
            else:
                print("❌ Impossible de récupérer les données météo", file=sys.stderr)
                sys.exit(1)
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt du producteur...")
    finally:
        producer.close()
        print("✅ Producteur fermé.")


if __name__ == '__main__':
    main()
