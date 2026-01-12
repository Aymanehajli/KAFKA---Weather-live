#!/usr/bin/env python3
"""
Visualisation et agrégation des logs météo - Exercice 8
Lit les données depuis HDFS et crée des visualisations.
"""

import argparse
import json
import sys
import subprocess
import os
from datetime import datetime
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import pandas as pd
from pathlib import Path


def read_hdfs_file(hdfs_path):
    """
    Lit un fichier depuis HDFS.
    
    Args:
        hdfs_path: Chemin HDFS du fichier
    
    Returns:
        list: Liste des lignes du fichier (chaque ligne est un JSON)
    """
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-cat', hdfs_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            return []
        
        data = []
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        
        return data
    except Exception as e:
        print(f"⚠️  Erreur lors de la lecture de {hdfs_path}: {e}", file=sys.stderr)
        return []


def list_hdfs_directories(hdfs_base_path="/hdfs-data"):
    """
    Liste tous les fichiers alerts.json dans HDFS.
    
    Args:
        hdfs_base_path: Chemin de base HDFS
    
    Returns:
        list: Liste des chemins HDFS des fichiers alerts.json
    """
    try:
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '-R', hdfs_base_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            return []
        
        files = []
        for line in result.stdout.strip().split('\n'):
            if 'alerts.json' in line:
                # Extraire le chemin HDFS
                parts = line.split()
                if len(parts) >= 8:
                    files.append(parts[-1])
        
        return files
    except Exception as e:
        print(f"⚠️  Erreur lors de la liste HDFS: {e}", file=sys.stderr)
        return []


def load_all_alerts_from_hdfs(hdfs_base_path="/hdfs-data"):
    """
    Charge toutes les alertes depuis HDFS.
    
    Args:
        hdfs_base_path: Chemin de base HDFS
    
    Returns:
        list: Liste de toutes les alertes
    """
    print(f"📂 Chargement des alertes depuis HDFS: {hdfs_base_path}")
    
    files = list_hdfs_directories(hdfs_base_path)
    
    if not files:
        print("⚠️  Aucun fichier alerts.json trouvé dans HDFS")
        return []
    
    print(f"📄 {len(files)} fichier(s) trouvé(s)")
    
    all_alerts = []
    for file_path in files:
        data = read_hdfs_file(file_path)
        for item in data:
            if 'alert_data' in item:
                alert = item['alert_data']
                alert['saved_timestamp'] = item.get('timestamp', '')
                all_alerts.append(alert)
    
    print(f"✅ {len(all_alerts)} alerte(s) chargée(s)")
    return all_alerts


def load_data_from_kafka_consumer(topic='weather_transformed', limit=1000):
    """
    Charge les données depuis Kafka via un consommateur temporaire.
    
    Args:
        topic: Nom du topic Kafka
        limit: Nombre maximum de messages à lire
    
    Returns:
        list: Liste des données météo
    """
    try:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError
        
        print(f"📡 Chargement des données depuis Kafka: {topic}")
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers='localhost:29092',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: x.decode('utf-8'),
            consumer_timeout_ms=5000
        )
        
        data = []
        count = 0
        for message in consumer:
            if count >= limit:
                break
            try:
                data.append(json.loads(message.value))
                count += 1
            except json.JSONDecodeError:
                continue
        
        consumer.close()
        print(f"✅ {len(data)} message(s) chargé(s) depuis Kafka")
        return data
    except Exception as e:
        print(f"⚠️  Erreur lors de la lecture depuis Kafka: {e}", file=sys.stderr)
        return []


def create_temperature_evolution_plot(data, output_dir='output'):
    """
    Crée un graphique d'évolution de la température au fil du temps.
    
    Args:
        data: Liste des données météo
        output_dir: Répertoire de sortie
    """
    if not data:
        print("⚠️  Aucune donnée pour créer le graphique de température")
        return
    
    # Préparer les données
    df_data = []
    for item in data:
        if 'temperature' in item and 'event_time' in item:
            try:
                df_data.append({
                    'timestamp': pd.to_datetime(item['event_time']),
                    'temperature': float(item['temperature']),
                    'city': item.get('city', 'Unknown'),
                    'country': item.get('country', 'Unknown')
                })
            except (ValueError, KeyError):
                continue
    
    if not df_data:
        print("⚠️  Aucune donnée valide pour le graphique de température")
        return
    
    df = pd.DataFrame(df_data)
    df = df.sort_values('timestamp')
    
    # Créer le graphique
    plt.figure(figsize=(14, 6))
    
    # Grouper par ville si plusieurs villes
    if df['city'].nunique() > 1:
        for city in df['city'].unique():
            city_data = df[df['city'] == city]
            plt.plot(city_data['timestamp'], city_data['temperature'], 
                    marker='o', label=city, alpha=0.7, markersize=4)
    else:
        plt.plot(df['timestamp'], df['temperature'], marker='o', 
                color='blue', alpha=0.7, markersize=4)
    
    plt.title('Évolution de la température au fil du temps', fontsize=16, fontweight='bold')
    plt.xlabel('Temps', fontsize=12)
    plt.ylabel('Température (°C)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Formater l'axe des dates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'temperature_evolution.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Graphique sauvegardé: {output_path}")
    plt.close()


def create_windspeed_evolution_plot(data, output_dir='output'):
    """
    Crée un graphique d'évolution de la vitesse du vent au fil du temps.
    
    Args:
        data: Liste des données météo
        output_dir: Répertoire de sortie
    """
    if not data:
        print("⚠️  Aucune donnée pour créer le graphique de vent")
        return
    
    # Préparer les données
    df_data = []
    for item in data:
        if 'windspeed' in item and 'event_time' in item:
            try:
                df_data.append({
                    'timestamp': pd.to_datetime(item['event_time']),
                    'windspeed': float(item['windspeed']),
                    'city': item.get('city', 'Unknown'),
                    'country': item.get('country', 'Unknown')
                })
            except (ValueError, KeyError):
                continue
    
    if not df_data:
        print("⚠️  Aucune donnée valide pour le graphique de vent")
        return
    
    df = pd.DataFrame(df_data)
    df = df.sort_values('timestamp')
    
    # Créer le graphique
    plt.figure(figsize=(14, 6))
    
    # Grouper par ville si plusieurs villes
    if df['city'].nunique() > 1:
        for city in df['city'].unique():
            city_data = df[df['city'] == city]
            plt.plot(city_data['timestamp'], city_data['windspeed'], 
                    marker='s', label=city, alpha=0.7, markersize=4)
    else:
        plt.plot(df['timestamp'], df['windspeed'], marker='s', 
                color='green', alpha=0.7, markersize=4)
    
    plt.title('Évolution de la vitesse du vent au fil du temps', fontsize=16, fontweight='bold')
    plt.xlabel('Temps', fontsize=12)
    plt.ylabel('Vitesse du vent (m/s)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Formater l'axe des dates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'windspeed_evolution.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Graphique sauvegardé: {output_path}")
    plt.close()


def create_alerts_by_level_plot(data, output_dir='output'):
    """
    Crée un graphique du nombre d'alertes vent et chaleur par niveau.
    
    Args:
        data: Liste des données météo
        output_dir: Répertoire de sortie
    """
    if not data:
        print("⚠️  Aucune donnée pour créer le graphique d'alertes")
        return
    
    # Compter les alertes
    wind_alerts = defaultdict(int)
    heat_alerts = defaultdict(int)
    
    for item in data:
        wind_level = item.get('wind_alert_level', 'level_0')
        heat_level = item.get('heat_alert_level', 'level_0')
        
        if wind_level in ['level_1', 'level_2']:
            wind_alerts[wind_level] += 1
        if heat_level in ['level_1', 'level_2']:
            heat_alerts[heat_level] += 1
    
    # Créer le graphique
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Graphique des alertes de vent
    if wind_alerts:
        levels = sorted(wind_alerts.keys())
        counts = [wind_alerts[level] for level in levels]
        colors = ['orange' if l == 'level_1' else 'red' for l in levels]
        
        ax1.bar(levels, counts, color=colors, alpha=0.7, edgecolor='black')
        ax1.set_title('Alertes de vent par niveau', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Niveau d\'alerte', fontsize=12)
        ax1.set_ylabel('Nombre d\'alertes', fontsize=12)
        ax1.grid(True, alpha=0.3, axis='y')
        
        # Ajouter les valeurs sur les barres
        for i, (level, count) in enumerate(zip(levels, counts)):
            ax1.text(level, count, str(count), ha='center', va='bottom', fontweight='bold')
    else:
        ax1.text(0.5, 0.5, 'Aucune alerte de vent', ha='center', va='center', transform=ax1.transAxes)
        ax1.set_title('Alertes de vent par niveau', fontsize=14, fontweight='bold')
    
    # Graphique des alertes de chaleur
    if heat_alerts:
        levels = sorted(heat_alerts.keys())
        counts = [heat_alerts[level] for level in levels]
        colors = ['orange' if l == 'level_1' else 'red' for l in levels]
        
        ax2.bar(levels, counts, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_title('Alertes de chaleur par niveau', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Niveau d\'alerte', fontsize=12)
        ax2.set_ylabel('Nombre d\'alertes', fontsize=12)
        ax2.grid(True, alpha=0.3, axis='y')
        
        # Ajouter les valeurs sur les barres
        for i, (level, count) in enumerate(zip(levels, counts)):
            ax2.text(level, count, str(count), ha='center', va='bottom', fontweight='bold')
    else:
        ax2.text(0.5, 0.5, 'Aucune alerte de chaleur', ha='center', va='center', transform=ax2.transAxes)
        ax2.set_title('Alertes de chaleur par niveau', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'alerts_by_level.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Graphique sauvegardé: {output_path}")
    plt.close()


def create_weathercode_by_country_plot(data, output_dir='output'):
    """
    Crée un graphique du code météo le plus fréquent par pays.
    
    Args:
        data: Liste des données météo
        output_dir: Répertoire de sortie
    """
    if not data:
        print("⚠️  Aucune donnée pour créer le graphique de codes météo")
        return
    
    # Compter les codes météo par pays
    country_weathercodes = defaultdict(lambda: defaultdict(int))
    
    for item in data:
        country = item.get('country', 'Unknown')
        weathercode = item.get('weathercode')
        
        if weathercode is not None:
            country_weathercodes[country][weathercode] += 1
    
    if not country_weathercodes:
        print("⚠️  Aucune donnée valide pour le graphique de codes météo")
        return
    
    # Trouver le code le plus fréquent par pays
    country_top_code = {}
    for country, codes in country_weathercodes.items():
        if codes:
            top_code = max(codes.items(), key=lambda x: x[1])
            country_top_code[country] = {
                'code': top_code[0],
                'count': top_code[1],
                'total': sum(codes.values())
            }
    
    # Créer le graphique
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Graphique 1: Code le plus fréquent par pays
    countries = list(country_top_code.keys())
    codes = [country_top_code[c]['code'] for c in countries]
    counts = [country_top_code[c]['count'] for c in countries]
    
    bars = ax1.barh(countries, codes, color='steelblue', alpha=0.7, edgecolor='black')
    ax1.set_title('Code météo le plus fréquent par pays', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Code météo', fontsize=12)
    ax1.set_ylabel('Pays', fontsize=12)
    ax1.grid(True, alpha=0.3, axis='x')
    
    # Ajouter les valeurs sur les barres
    for i, (country, code, count) in enumerate(zip(countries, codes, counts)):
        ax1.text(code, i, f'{code} ({count})', va='center', ha='left', fontweight='bold')
    
    # Graphique 2: Distribution des codes météo par pays (heatmap)
    # Préparer les données pour la heatmap
    all_codes = set()
    for codes_dict in country_weathercodes.values():
        all_codes.update(codes_dict.keys())
    
    all_codes = sorted(all_codes)
    heatmap_data = []
    heatmap_countries = []
    
    for country in sorted(country_weathercodes.keys()):
        heatmap_countries.append(country)
        row = []
        for code in all_codes:
            count = country_weathercodes[country].get(code, 0)
            row.append(count)
        heatmap_data.append(row)
    
    if heatmap_data:
        sns.heatmap(heatmap_data, 
                   xticklabels=all_codes,
                   yticklabels=heatmap_countries,
                   annot=True, 
                   fmt='d',
                   cmap='YlOrRd',
                   ax=ax2,
                   cbar_kws={'label': 'Fréquence'})
        ax2.set_title('Distribution des codes météo par pays', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Code météo', fontsize=12)
        ax2.set_ylabel('Pays', fontsize=12)
    
    plt.tight_layout()
    
    output_path = os.path.join(output_dir, 'weathercode_by_country.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Graphique sauvegardé: {output_path}")
    plt.close()


def main():
    """Fonction principale."""
    parser = argparse.ArgumentParser(
        description='Visualisation et agrégation des logs météo',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  %(prog)s --source hdfs
  %(prog)s --source kafka --limit 500
  %(prog)s --source hdfs --output-dir visualizations
        """
    )
    
    parser.add_argument(
        '--source',
        choices=['hdfs', 'kafka'],
        default='hdfs',
        help='Source des données (défaut: hdfs)'
    )
    
    parser.add_argument(
        '--hdfs-path',
        default='/hdfs-data',
        help='Chemin HDFS de base (défaut: /hdfs-data)'
    )
    
    parser.add_argument(
        '--kafka-topic',
        default='weather_transformed',
        help='Topic Kafka (défaut: weather_transformed)'
    )
    
    parser.add_argument(
        '--kafka-limit',
        type=int,
        default=1000,
        help='Nombre maximum de messages à lire depuis Kafka (défaut: 1000)'
    )
    
    parser.add_argument(
        '--output-dir',
        default='output',
        help='Répertoire de sortie pour les graphiques (défaut: output)'
    )
    
    args = parser.parse_args()
    
    # Créer le répertoire de sortie
    os.makedirs(args.output_dir, exist_ok=True)
    
    print("=" * 60)
    print("📊 Visualisation des données météo")
    print("=" * 60)
    print()
    
    # Charger les données
    if args.source == 'hdfs':
        data = load_all_alerts_from_hdfs(args.hdfs_path)
    else:
        data = load_data_from_kafka_consumer(args.kafka_topic, args.kafka_limit)
    
    if not data:
        print("❌ Aucune donnée disponible pour la visualisation", file=sys.stderr)
        sys.exit(1)
    
    print()
    print("📈 Création des visualisations...")
    print()
    
    # Créer les visualisations
    create_temperature_evolution_plot(data, args.output_dir)
    create_windspeed_evolution_plot(data, args.output_dir)
    create_alerts_by_level_plot(data, args.output_dir)
    create_weathercode_by_country_plot(data, args.output_dir)
    
    print()
    print("=" * 60)
    print("✅ Visualisations terminées!")
    print(f"📁 Graphiques sauvegardés dans: {args.output_dir}/")
    print("=" * 60)


if __name__ == '__main__':
    main()
