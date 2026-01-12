#!/usr/bin/env python3
"""
Consommateur Kafka pour sauvegarder les alertes dans HDFS - Exercice 7
Lit depuis weather_transformed et sauvegarde les alertes dans HDFS avec structure organisée.
"""

import argparse
import json
import sys
import os
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import subprocess


def check_hdfs_available():
    """Vérifie si HDFS est accessible."""
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/'],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def create_hdfs_directory(hdfs_path):
    """
    Crée un répertoire dans HDFS s'il n'existe pas.
    
    Args:
        hdfs_path: Chemin HDFS (ex: /hdfs-data/France/Paris)
    """
    try:
        # Vérifier si le répertoire existe
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-test', '-d', hdfs_path],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        # Si le répertoire n'existe pas, le créer
        if result.returncode != 0:
            subprocess.run(
                ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', hdfs_path],
                check=True,
                capture_output=True,
                text=True,
                timeout=10
            )
            print(f"📁 Répertoire HDFS créé: {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"⚠️  Erreur lors de la création du répertoire HDFS {hdfs_path}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"⚠️  Erreur inattendue lors de la création du répertoire: {e}", file=sys.stderr)


def save_alert_to_hdfs(alert_data, country, city, hdfs_base_path="/hdfs-data"):
    """
    Sauvegarde une alerte dans HDFS.
    
    Args:
        alert_data: Données de l'alerte (dict)
        country: Nom du pays
        city: Nom de la ville
        hdfs_base_path: Chemin de base HDFS (défaut: /hdfs-data)
    """
    try:
        # Nettoyer les noms pour éviter les caractères problématiques
        country_clean = country.replace('/', '_').replace(' ', '_') if country else "Unknown"
        city_clean = city.replace('/', '_').replace(' ', '_') if city else "Unknown"
        
        # Construire le chemin HDFS
        hdfs_dir = f"{hdfs_base_path}/{country_clean}/{city_clean}"
        hdfs_file = f"{hdfs_dir}/alerts.json"
        
        # Créer le répertoire si nécessaire
        create_hdfs_directory(hdfs_dir)
        
        # Préparer les données à ajouter
        alert_entry = {
            "timestamp": datetime.now().isoformat(),
            "alert_data": alert_data
        }
        
        # Lire le fichier existant s'il existe, sinon créer une liste vide
        existing_alerts = []
        try:
            result = subprocess.run(
                ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-cat', hdfs_file],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                # Parser le contenu existant (peut être un JSON par ligne ou un tableau)
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if line.strip():
                        try:
                            existing_alerts.append(json.loads(line))
                        except json.JSONDecodeError:
                            # Essayer de parser comme tableau JSON
                            try:
                                existing_alerts = json.loads(result.stdout)
                                break
                            except json.JSONDecodeError:
                                pass
        except Exception:
            # Fichier n'existe pas encore, c'est normal
            pass
        
        # Ajouter la nouvelle alerte
        existing_alerts.append(alert_entry)
        
        # Écrire dans un fichier temporaire local
        temp_file = f"/tmp/alerts_{country_clean}_{city_clean}_{datetime.now().timestamp()}.json"
        with open(temp_file, 'w') as f:
            # Écrire en format JSON Lines (une alerte par ligne)
            for alert in existing_alerts:
                f.write(json.dumps(alert) + '\n')
        
        # Copier le fichier dans HDFS
        subprocess.run(
            ['docker', 'cp', temp_file, f'namenode:/tmp/alerts_temp.json'],
            check=True,
            capture_output=True,
            timeout=10
        )
        
        # Déplacer le fichier dans HDFS (écrase l'ancien)
        subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', '-f', '/tmp/alerts_temp.json', hdfs_file],
            check=True,
            capture_output=True,
            timeout=10
        )
        
        # Nettoyer le fichier temporaire
        os.remove(temp_file)
        subprocess.run(
            ['docker', 'exec', 'namenode', 'rm', '-f', '/tmp/alerts_temp.json'],
            capture_output=True
        )
        
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur lors de l'écriture dans HDFS: {e}", file=sys.stderr)
        if e.stdout:
            print(f"   stdout: {e.stdout}", file=sys.stderr)
        if e.stderr:
            print(f"   stderr: {e.stderr}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"❌ Erreur inattendue lors de la sauvegarde: {e}", file=sys.stderr)
        return False


def is_alert(weather_data):
    """
    Vérifie si un message contient une alerte (level_1 ou level_2).
    
    Args:
        weather_data: Données météo transformées (dict)
    
    Returns:
        bool: True si le message contient une alerte
    """
    wind_alert = weather_data.get('wind_alert_level', 'level_0')
    heat_alert = weather_data.get('heat_alert_level', 'level_0')
    
    return wind_alert in ['level_1', 'level_2'] or heat_alert in ['level_1', 'level_2']


def create_consumer(topic, bootstrap_servers='localhost:29092', group_id='hdfs-alert-saver'):
    """
    Crée et configure un consommateur Kafka.
    
    Args:
        topic: Nom du topic Kafka à consommer
        bootstrap_servers: Adresse du serveur Kafka
        group_id: ID du groupe de consommateurs
    
    Returns:
        KafkaConsumer configuré
    """
    consumer_config = {
        'bootstrap_servers': bootstrap_servers,
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
        'value_deserializer': lambda x: x.decode('utf-8'),
        'group_id': group_id,
    }
    
    try:
        consumer = KafkaConsumer(topic, **consumer_config)
        return consumer
    except KafkaError as e:
        print(f"❌ Erreur lors de la création du consommateur: {e}", file=sys.stderr)
        sys.exit(1)


def consume_and_save_alerts(consumer, topic, hdfs_base_path="/hdfs-data"):
    """
    Consomme les messages et sauvegarde les alertes dans HDFS.
    
    Args:
        consumer: Instance KafkaConsumer
        topic: Nom du topic (pour affichage)
        hdfs_base_path: Chemin de base HDFS
    """
    print(f"📡 Connexion au topic '{topic}'...")
    print(f"💾 Sauvegarde dans HDFS: {hdfs_base_path}/{{country}}/{{city}}/alerts.json")
    print(f"✅ Connecté! En attente de messages avec alertes... (Ctrl+C pour arrêter)\n")
    print("-" * 60)
    
    alert_count = 0
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            # Parser le message JSON
            try:
                weather_data = json.loads(message.value)
            except json.JSONDecodeError as e:
                print(f"⚠️  Erreur de parsing JSON: {e}", file=sys.stderr)
                continue
            
            # Vérifier si c'est une alerte
            if is_alert(weather_data):
                alert_count += 1
                
                # Extraire les informations de localisation
                country = weather_data.get('country', 'Unknown')
                city = weather_data.get('city', 'Unknown')
                
                # Si pas de ville/pays, utiliser les coordonnées comme fallback
                if country == 'Unknown' or city == 'Unknown':
                    lat = weather_data.get('latitude', 0)
                    lon = weather_data.get('longitude', 0)
                    country = f"Lat_{lat}"
                    city = f"Lon_{lon}"
                
                # Sauvegarder dans HDFS
                print(f"\n🚨 Alerte détectée #{alert_count}:")
                print(f"   Localisation: {city}, {country}")
                print(f"   Vent: {weather_data.get('wind_alert_level', 'N/A')}")
                print(f"   Chaleur: {weather_data.get('heat_alert_level', 'N/A')}")
                print(f"   Température: {weather_data.get('temperature', 'N/A')}°C")
                print(f"   Vent: {weather_data.get('windspeed', 'N/A')} m/s")
                
                if save_alert_to_hdfs(weather_data, country, city, hdfs_base_path):
                    print(f"   ✅ Sauvegardé dans HDFS: {hdfs_base_path}/{country}/{city}/alerts.json")
                else:
                    print(f"   ❌ Échec de la sauvegarde")
                
                print("-" * 60)
            else:
                # Afficher un point pour indiquer qu'un message a été traité (sans alerte)
                if message_count % 10 == 0:
                    print(f"📊 {message_count} message(s) traité(s), {alert_count} alerte(s) trouvée(s)")
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt du consommateur...")
        print(f"📊 Statistiques: {message_count} message(s) traité(s), {alert_count} alerte(s) sauvegardée(s)")
    except KafkaError as e:
        print(f"\n❌ Erreur Kafka: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erreur inattendue: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        consumer.close()
        print("✅ Consommateur fermé.")


def main():
    """Fonction principale."""
    parser = argparse.ArgumentParser(
        description='Consommateur Kafka pour sauvegarder les alertes dans HDFS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  %(prog)s
  %(prog)s --topic weather_transformed
  %(prog)s --hdfs-path /data/alerts
        """
    )
    
    parser.add_argument(
        '--topic',
        default='weather_transformed',
        help='Nom du topic Kafka à consommer (défaut: weather_transformed)'
    )
    
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:29092',
        help='Adresse du serveur Kafka (défaut: localhost:29092)'
    )
    
    parser.add_argument(
        '--group-id',
        default='hdfs-alert-saver',
        help='ID du groupe de consommateurs (défaut: hdfs-alert-saver)'
    )
    
    parser.add_argument(
        '--hdfs-path',
        default='/hdfs-data',
        help='Chemin de base HDFS (défaut: /hdfs-data)'
    )
    
    args = parser.parse_args()
    
    # Vérifier que HDFS est accessible
    print("🔍 Vérification de l'accès HDFS...")
    if not check_hdfs_available():
        print("❌ Erreur: HDFS n'est pas accessible.", file=sys.stderr)
        print("   Assurez-vous que le conteneur namenode est en cours d'exécution:", file=sys.stderr)
        print("   docker ps | grep namenode", file=sys.stderr)
        sys.exit(1)
    print("✅ HDFS est accessible")
    print()
    
    # Créer le répertoire de base dans HDFS
    create_hdfs_directory(args.hdfs_path)
    
    # Créer le consommateur
    consumer = create_consumer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id
    )
    
    # Consommer et sauvegarder les alertes
    consume_and_save_alerts(consumer, args.topic, args.hdfs_path)


if __name__ == '__main__':
    main()
