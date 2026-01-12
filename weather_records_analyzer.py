#!/usr/bin/env python3
"""
Job Spark pour détecter les records climatiques locaux - Exercice 10
Analyse les données historiques dans HDFS et calcule les records pour chaque ville.
"""

import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as spark_max, min as spark_min, struct, to_json,
    when, sum as spark_sum, count, desc, row_number, lit, first
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
)
from pyspark.sql.window import Window


def create_spark_session(app_name="WeatherRecordsAnalyzer"):
    """Crée une session Spark avec les configurations nécessaires."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_historical_data_from_hdfs(spark, hdfs_base_path="/hdfs-data"):
    """
    Lit les données historiques depuis HDFS.
    
    Args:
        spark: SparkSession
        hdfs_base_path: Chemin de base HDFS
    
    Returns:
        DataFrame: Données historiques avec métadonnées
    """
    print(f"📂 Lecture des données historiques depuis HDFS: {hdfs_base_path}")
    
    # Lire les fichiers JSON depuis HDFS
    # Note: Spark peut lire directement depuis HDFS avec le format text
    try:
        # Lister tous les fichiers historical_data.json
        import subprocess
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '-R', hdfs_base_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        historical_files = []
        for line in result.stdout.strip().split('\n'):
            if 'weather_history_raw/historical_data.json' in line:
                parts = line.split()
                if len(parts) >= 8:
                    hdfs_path = parts[-1]
                    historical_files.append(hdfs_path)
        
        if not historical_files:
            print("⚠️  Aucun fichier historique trouvé dans HDFS")
            return None
        
        print(f"📄 {len(historical_files)} fichier(s) historique(s) trouvé(s)")
        
        # Lire et parser chaque fichier
        all_data = []
        for hdfs_file in historical_files:
            try:
                result = subprocess.run(
                    ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-cat', hdfs_file],
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                if result.returncode == 0:
                    data = json.loads(result.stdout)
                    
                    # Extraire les métadonnées et les données
                    metadata = data.get('metadata', {})
                    location = metadata.get('location', {})
                    daily_data = data.get('data', {}).get('daily', {})
                    
                    # Créer un DataFrame à partir des données quotidiennes
                    if 'time' in daily_data:
                        times = daily_data.get('time', [])
                        temp_max = daily_data.get('temperature_2m_max', [])
                        temp_min = daily_data.get('temperature_2m_min', [])
                        temp_mean = daily_data.get('temperature_2m_mean', [])
                        precip = daily_data.get('precipitation_sum', [])
                        wind_max = daily_data.get('windspeed_10m_max', [])
                        wind_mean = daily_data.get('windspeed_10m_mean', [])
                        
                        for i in range(len(times)):
                            if times[i]:
                                all_data.append({
                                    'date': times[i],
                                    'city': location.get('city', 'Unknown'),
                                    'country': location.get('country', 'Unknown'),
                                    'latitude': location.get('latitude', 0.0),
                                    'longitude': location.get('longitude', 0.0),
                                    'temperature_max': temp_max[i] if i < len(temp_max) else None,
                                    'temperature_min': temp_min[i] if i < len(temp_min) else None,
                                    'temperature_mean': temp_mean[i] if i < len(temp_mean) else None,
                                    'precipitation': precip[i] if i < len(precip) else None,
                                    'windspeed_max': wind_max[i] if i < len(wind_max) else None,
                                    'windspeed_mean': wind_mean[i] if i < len(wind_mean) else None,
                                })
            except Exception as e:
                print(f"⚠️  Erreur lors de la lecture de {hdfs_file}: {e}", file=sys.stderr)
                continue
        
        if not all_data:
            print("⚠️  Aucune donnée valide trouvée")
            return None
        
        # Créer un DataFrame Spark
        df = spark.createDataFrame(all_data)
        print(f"✅ {df.count()} jour(s) de données chargé(s)")
        return df
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture HDFS: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return None


def calculate_records(df):
    """
    Calcule les records climatiques pour chaque ville.
    
    Args:
        df: DataFrame avec les données historiques
    
    Returns:
        DataFrame: Records calculés par ville
    """
    print("\n📊 Calcul des records climatiques...")
    
    # Filtrer les valeurs nulles
    df_clean = df.filter(
        col('temperature_max').isNotNull() &
        col('temperature_min').isNotNull() &
        col('windspeed_max').isNotNull() &
        col('precipitation').isNotNull()
    )
    
    # Utiliser des fenêtres pour obtenir les records avec leurs dates
    # Jour le plus chaud
    window_max_temp = Window.partitionBy('country', 'city').orderBy(desc('temperature_max'), desc('date'))
    max_temp = df_clean.withColumn('rn', row_number().over(window_max_temp)).filter(col('rn') == 1).select(
        'country', 'city', 'latitude', 'longitude',
        col('date').alias('max_temp_date'),
        col('temperature_max').alias('max_temperature')
    )
    
    # Jour le plus froid
    window_min_temp = Window.partitionBy('country', 'city').orderBy(col('temperature_min'), desc('date'))
    min_temp = df_clean.withColumn('rn', row_number().over(window_min_temp)).filter(col('rn') == 1).select(
        'country', 'city',
        col('date').alias('min_temp_date'),
        col('temperature_min').alias('min_temperature')
    )
    
    # Rafale de vent la plus forte
    window_max_wind = Window.partitionBy('country', 'city').orderBy(desc('windspeed_max'), desc('date'))
    max_wind = df_clean.withColumn('rn', row_number().over(window_max_wind)).filter(col('rn') == 1).select(
        'country', 'city',
        col('date').alias('max_wind_date'),
        col('windspeed_max').alias('max_windspeed')
    )
    
    # Jour le plus pluvieux
    window_max_precip = Window.partitionBy('country', 'city').orderBy(desc('precipitation'), desc('date'))
    max_precip = df_clean.withColumn('rn', row_number().over(window_max_precip)).filter(col('rn') == 1).select(
        'country', 'city',
        col('date').alias('max_precip_date'),
        col('precipitation').alias('max_precipitation')
    )
    
    # Joindre tous les records
    records = max_temp.join(min_temp, ['country', 'city'], 'outer') \
        .join(max_wind, ['country', 'city'], 'outer') \
        .join(max_precip, ['country', 'city'], 'outer')
    
    # Ajouter le nombre total de jours
    total_days = df_clean.groupBy('country', 'city').agg(count('*').alias('total_days'))
    records = records.join(total_days, ['country', 'city'], 'left')
    
    return records


def save_records_to_hdfs(records_df, hdfs_base_path="/hdfs-data"):
    """
    Sauvegarde les records dans HDFS.
    
    Args:
        records_df: DataFrame avec les records
        hdfs_base_path: Chemin de base HDFS
    """
    print("\n💾 Sauvegarde des records dans HDFS...")
    
    records_list = records_df.collect()
    
    for record in records_list:
        country = record['country'] or 'Unknown'
        city = record['city'] or 'Unknown'
        
        country_clean = country.replace('/', '_').replace(' ', '_')
        city_clean = city.replace('/', '_').replace(' ', '_')
        
        hdfs_dir = f"{hdfs_base_path}/{country_clean}/{city_clean}/weather_records"
        hdfs_file = f"{hdfs_dir}/records.json"
        
        # Préparer les données du record
        record_data = {
            'city': city,
            'country': country,
            'latitude': record.get('latitude', 0.0),
            'longitude': record.get('longitude', 0.0),
            'records': {
                'hottest_day': {
                    'date': record.get('max_temp_date'),
                    'temperature': record.get('max_temperature')
                },
                'coldest_day': {
                    'date': record.get('min_temp_date'),
                    'temperature': record.get('min_temperature')
                },
                'strongest_wind': {
                    'date': record.get('max_wind_date'),
                    'windspeed': record.get('max_windspeed')
                },
                'rainiest_day': {
                    'date': record.get('max_precip_date'),
                    'precipitation': record.get('max_precipitation')
                }
            },
            'statistics': {
                'total_days': record.get('total_days', 0)
            }
        }
        
        try:
            import subprocess
            import os
            
            # Créer le répertoire
            subprocess.run(
                ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', hdfs_dir],
                check=True,
                capture_output=True,
                timeout=10
            )
            
            # Écrire dans un fichier temporaire
            temp_file = f"/tmp/weather_records_{country_clean}_{city_clean}.json"
            with open(temp_file, 'w') as f:
                json.dump(record_data, f, indent=2)
            
            # Copier dans le conteneur
            subprocess.run(
                ['docker', 'cp', temp_file, f'namenode:/tmp/weather_records_temp.json'],
                check=True,
                capture_output=True,
                timeout=10
            )
            
            # Déplacer dans HDFS
            subprocess.run(
                ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', '-f', '/tmp/weather_records_temp.json', hdfs_file],
                check=True,
                capture_output=True,
                timeout=10
            )
            
            # Nettoyer
            os.remove(temp_file)
            subprocess.run(
                ['docker', 'exec', 'namenode', 'rm', '-f', '/tmp/weather_records_temp.json'],
                capture_output=True
            )
            
            print(f"✅ Records sauvegardés: {hdfs_file}")
            
        except Exception as e:
            print(f"⚠️  Erreur lors de la sauvegarde pour {city}, {country}: {e}", file=sys.stderr)


def send_records_to_kafka(records_df, kafka_bootstrap_servers, topic='weather_records'):
    """
    Envoie les records à Kafka.
    
    Args:
        records_df: DataFrame avec les records
        kafka_bootstrap_servers: Serveurs Kafka
        topic: Topic Kafka
    """
    print(f"\n📤 Envoi des records à Kafka: {topic}...")
    
    # Convertir en JSON pour chaque record
    records_list = records_df.collect()
    
    try:
        from kafka import KafkaProducer
        from kafka.errors import KafkaError
        
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )
        
        for record in records_list:
            country = record['country'] or 'Unknown'
            city = record['city'] or 'Unknown'
            
            record_data = {
                'city': city,
                'country': country,
                'latitude': record.get('latitude', 0.0),
                'longitude': record.get('longitude', 0.0),
                'records': {
                    'hottest_day': {
                        'date': record.get('max_temp_date'),
                        'temperature': record.get('max_temperature')
                    },
                    'coldest_day': {
                        'date': record.get('min_temp_date'),
                        'temperature': record.get('min_temperature')
                    },
                    'strongest_wind': {
                        'date': record.get('max_wind_date'),
                        'windspeed': record.get('max_windspeed')
                    },
                    'rainiest_day': {
                        'date': record.get('max_precip_date'),
                        'precipitation': record.get('max_precipitation')
                    }
                },
                'statistics': {
                    'total_days': record.get('total_days', 0)
                },
                'computed_at': datetime.now().isoformat()
            }
            
            location_key = f"{country}/{city}"
            producer.send(topic, key=location_key, value=record_data)
            print(f"✅ Record envoyé: {city}, {country}")
        
        producer.flush()
        producer.close()
        print(f"✅ {len(records_list)} record(s) envoyé(s) à Kafka")
        
    except Exception as e:
        print(f"❌ Erreur lors de l'envoi à Kafka: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()


def main():
    """Fonction principale."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Analyseur de records climatiques - Analyse HDFS et calcule les records',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  %(prog)s
  %(prog)s --hdfs-path /hdfs-data
  %(prog)s --skip-kafka
        """
    )
    
    parser.add_argument(
        '--hdfs-path',
        default='/hdfs-data',
        help='Chemin HDFS de base (défaut: /hdfs-data)'
    )
    
    parser.add_argument(
        '--kafka-servers',
        default='localhost:29092',
        help='Serveurs Kafka bootstrap (défaut: localhost:29092)'
    )
    
    parser.add_argument(
        '--kafka-topic',
        default='weather_records',
        help='Topic Kafka (défaut: weather_records)'
    )
    
    parser.add_argument(
        '--skip-kafka',
        action='store_true',
        help='Ne pas envoyer à Kafka, seulement HDFS'
    )
    
    parser.add_argument(
        '--skip-hdfs',
        action='store_true',
        help='Ne pas sauvegarder dans HDFS, seulement Kafka'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("📊 Analyseur de records climatiques")
    print("=" * 60)
    print()
    
    # Créer la session Spark
    spark = create_spark_session()
    
    try:
        # Lire les données historiques depuis HDFS
        df = read_historical_data_from_hdfs(spark, args.hdfs_path)
        
        if df is None or df.count() == 0:
            print("❌ Aucune donnée historique disponible", file=sys.stderr)
            sys.exit(1)
        
        # Calculer les records
        records_df = calculate_records(df)
        
        if records_df is None or records_df.count() == 0:
            print("❌ Aucun record calculé", file=sys.stderr)
            sys.exit(1)
        
        print(f"\n✅ {records_df.count()} ville(s) analysée(s)")
        
        # Afficher un aperçu des records
        print("\n📋 Aperçu des records:")
        records_df.show(truncate=False)
        
        # Sauvegarder dans HDFS
        if not args.skip_hdfs:
            save_records_to_hdfs(records_df, args.hdfs_path)
        
        # Envoyer à Kafka
        if not args.skip_kafka:
            send_records_to_kafka(records_df, args.kafka_servers, args.kafka_topic)
        
        print("\n" + "=" * 60)
        print("✅ Analyse terminée!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Erreur: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
