#!/usr/bin/env python3
"""
Détecteur d'anomalies climatiques en temps réel - Exercice 13
Jointure Batch vs Speed : données temps réel (Kafka) vs profils historiques (HDFS)
"""

import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, when, struct, to_json, current_timestamp,
    month, abs as spark_abs, lit, expr, year as spark_year
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)
import subprocess
import os


def create_spark_session(app_name="AnomalyDetector"):
    """Crée une session Spark avec les configurations nécessaires."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_seasonal_profiles_from_hdfs(hdfs_base_path="/hdfs-data"):
    """
    Charge les profils saisonniers enrichis depuis HDFS.
    
    Args:
        hdfs_base_path: Chemin de base HDFS
    
    Returns:
        dict: Dictionnaire des profils par clé {country}/{city}/{month}
    """
    print(f"📂 Chargement des profils saisonniers depuis HDFS: {hdfs_base_path}")
    
    profiles_dict = {}
    
    try:
        # Lister tous les fichiers profile.json dans seasonal_profile_enriched
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '-R', hdfs_base_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        profile_files = []
        for line in result.stdout.strip().split('\n'):
            if 'seasonal_profile_enriched' in line and 'profile.json' in line:
                parts = line.split()
                if len(parts) >= 8:
                    hdfs_path = parts[-1]
                    profile_files.append(hdfs_path)
        
        if not profile_files:
            print("⚠️  Aucun profil saisonnier enrichi trouvé dans HDFS")
            print("   Exécutez d'abord l'exercice 12 pour créer les profils enrichis")
            return {}
        
        print(f"📄 {len(profile_files)} profil(s) trouvé(s)")
        
        # Charger chaque profil
        for profile_file in profile_files:
            try:
                result = subprocess.run(
                    ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-cat', profile_file],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    profile_data = json.loads(result.stdout)
                    country = profile_data.get('country', 'Unknown')
                    city = profile_data.get('city', 'Unknown')
                    
                    # Extraire les profils mensuels
                    monthly_profiles = profile_data.get('monthly_profiles', [])
                    for monthly in monthly_profiles:
                        month_num = monthly.get('month')
                        if month_num:
                            # Clé: country/city/month
                            key = f"{country}/{city}/{month_num}"
                            
                            if key not in profiles_dict:
                                profiles_dict[key] = {
                                    'country': country,
                                    'city': city,
                                    'month': month_num,
                                    'avg_temperature': monthly.get('avg_temperature'),
                                    'std_temperature': monthly.get('std_temperature'),
                                    'avg_windspeed': monthly.get('avg_windspeed'),
                                    'std_windspeed': monthly.get('std_windspeed'),
                                    'alert_probability': monthly.get('alert_probability', 0.0),
                                    'temp_q25': monthly.get('temp_q25'),
                                    'temp_median': monthly.get('temp_median'),
                                    'temp_q75': monthly.get('temp_q75'),
                                    'wind_q25': monthly.get('wind_q25'),
                                    'wind_median': monthly.get('wind_median'),
                                    'wind_q75': monthly.get('wind_q75'),
                                }
            except Exception as e:
                print(f"⚠️  Erreur lors de la lecture de {profile_file}: {e}", file=sys.stderr)
                continue
        
        print(f"✅ {len(profiles_dict)} profil(s) mensuel(s) chargé(s)")
        return profiles_dict
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement des profils: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return {}


def create_profiles_broadcast(spark, profiles_dict):
    """
    Crée un DataFrame broadcast avec les profils pour la jointure.
    
    Args:
        spark: SparkSession
        profiles_dict: Dictionnaire des profils
    
    Returns:
        DataFrame: DataFrame broadcastable avec les profils
    """
    if not profiles_dict:
        return None
    
    # Convertir le dictionnaire en liste pour créer un DataFrame
    profiles_list = []
    for key, profile in profiles_dict.items():
        profiles_list.append(profile)
    
    profiles_df = spark.createDataFrame(profiles_list)
    return profiles_df


def detect_anomalies(weather_df, profiles_df, temp_threshold=5.0, wind_threshold=2.0):
    """
    Détecte les anomalies en comparant les données temps réel aux profils historiques.
    
    Args:
        weather_df: DataFrame avec les données temps réel
        profiles_df: DataFrame avec les profils historiques
        temp_threshold: Seuil d'anomalie température en °C
        wind_threshold: Seuil d'anomalie vent en écarts-types
    
    Returns:
        DataFrame: Données avec anomalies détectées
    """
    print("\n🔍 Détection des anomalies...")
    
    # Extraire le mois de event_time
    weather_with_month = weather_df.withColumn(
        'month',
        expr("month(to_timestamp(event_time, 'yyyy-MM-dd\\'T\\'HH:mm:ss'))")
    ).withColumn(
        'year',
        expr("year(to_timestamp(event_time, 'yyyy-MM-dd\\'T\\'HH:mm:ss'))")
    )
    
    # Joindre avec les profils historiques
    joined_df = weather_with_month.join(
        profiles_df,
        [
            weather_with_month.country == profiles_df.country,
            weather_with_month.city == profiles_df.city,
            weather_with_month.month == profiles_df.month
        ],
        'left'
    )
    
    # Calculer les écarts et détecter les anomalies
    anomalies_df = joined_df.select(
        col('event_time'),
        weather_with_month.city.alias('city'),
        weather_with_month.country.alias('country'),
        weather_with_month.year.alias('year'),
        weather_with_month.month.alias('month'),
        
        # Température
        col('temperature').alias('observed_temperature'),
        profiles_df.avg_temperature.alias('expected_temperature'),
        (col('temperature') - profiles_df.avg_temperature).alias('temp_deviation'),
        abs(col('temperature') - profiles_df.avg_temperature).alias('temp_abs_deviation'),
        
        # Vent
        col('windspeed').alias('observed_windspeed'),
        profiles_df.avg_windspeed.alias('expected_windspeed'),
        (col('windspeed') - profiles_df.avg_windspeed).alias('wind_deviation'),
        profiles_df.std_windspeed.alias('wind_std'),
        
        # Alertes
        col('wind_alert_level'),
        col('heat_alert_level'),
        profiles_df.alert_probability.alias('expected_alert_probability'),
        
        # Profils de référence
        profiles_df.std_temperature.alias('temp_std'),
        profiles_df.temp_median.alias('temp_median'),
        profiles_df.wind_median.alias('wind_median')
    )
    
    # Définir les seuils d'anomalie
    # Température : écart > 5°C ou > 2 écarts-types
    # Vent : dépassement de 2 écarts-types
    # Alertes : écart significatif de fréquence
    
    anomalies_detected = anomalies_df.withColumn(
        'temp_anomaly',
        when(
            (col('temp_abs_deviation') > temp_threshold) |
            (col('temp_std').isNotNull() & (col('temp_abs_deviation') > wind_threshold * col('temp_std'))),
            True
        ).otherwise(False)
    ).withColumn(
        'wind_anomaly',
        when(
            (col('wind_std').isNotNull()) &
            (col('wind_deviation') > wind_threshold * col('wind_std')),
            True
        ).otherwise(False)
    ).withColumn(
        'alert_anomaly',
        when(
            ((col('wind_alert_level').isin(['level_1', 'level_2'])) |
             (col('heat_alert_level').isin(['level_1', 'level_2']))) &
            (col('expected_alert_probability') < 10),  # Probabilité historique < 10%
            True
        ).otherwise(False)
    ).withColumn(
        'is_anomaly',
        col('temp_anomaly') | col('wind_anomaly') | col('alert_anomaly')
    ).withColumn(
        'anomaly_type',
        when(col('temp_anomaly') & (col('temp_deviation') > 0), 'heat_wave')
        .when(col('temp_anomaly') & (col('temp_deviation') < 0), 'cold_spell')
        .when(col('wind_anomaly'), 'wind_storm')
        .when(col('alert_anomaly'), 'unexpected_alert')
        .otherwise('normal')
    )
    
    return anomalies_detected


def create_anomaly_messages(anomalies_df):
    """
    Crée les messages d'anomalie au format demandé.
    
    Args:
        anomalies_df: DataFrame avec les anomalies détectées
    
    Returns:
        DataFrame: Messages formatés pour Kafka
    """
    # Filtrer uniquement les anomalies
    anomalies_only = anomalies_df.filter(col('is_anomaly') == True)
    
    # Créer les messages pour chaque type d'anomalie
    temp_anomalies = anomalies_only.filter(col('temp_anomaly') == True).select(
        col('event_time').alias('event_time'),
        col('city'),
        col('country'),
        lit('temperature').alias('variable'),
        col('observed_temperature').alias('observed_value'),
        col('expected_temperature').alias('expected_value'),
        col('anomaly_type')
    )
    
    wind_anomalies = anomalies_only.filter(col('wind_anomaly') == True).select(
        col('event_time').alias('event_time'),
        col('city'),
        col('country'),
        lit('windspeed').alias('variable'),
        col('observed_windspeed').alias('observed_value'),
        col('expected_windspeed').alias('expected_value'),
        col('anomaly_type')
    )
    
    alert_anomalies = anomalies_only.filter(col('alert_anomaly') == True).select(
        col('event_time').alias('event_time'),
        col('city'),
        col('country'),
        lit('alert').alias('variable'),
        lit(1.0).alias('observed_value'),  # Alerte présente
        col('expected_alert_probability').alias('expected_value'),
        col('anomaly_type')
    )
    
    # Union de tous les types d'anomalies
    all_anomalies = temp_anomalies.union(wind_anomalies).union(alert_anomalies)
    
    return all_anomalies


def create_hdfs_saver(hdfs_base_path="/hdfs-data"):
    """
    Crée une fonction de sauvegarde HDFS pour les anomalies.
    
    Args:
        hdfs_base_path: Chemin de base HDFS
    
    Returns:
        function: Fonction foreachBatch pour sauvegarder dans HDFS
    """
    def foreach_batch_function(batch_df, batch_id):
        """Traite chaque batch et sauvegarde dans HDFS."""
        anomalies_list = batch_df.filter(col('is_anomaly') == True).collect()
        
        if not anomalies_list:
            return
        
        # Organiser par ville/année/mois
        city_year_month_anomalies = {}
        
        for anomaly in anomalies_list:
            country = anomaly['country'] or 'Unknown'
            city = anomaly['city'] or 'Unknown'
            year = anomaly.get('year', datetime.now().year)
            month = anomaly.get('month', datetime.now().month)
            
            key = f"{country}/{city}/{year}/{month}"
            
            if key not in city_year_month_anomalies:
                city_year_month_anomalies[key] = {
                    'country': country,
                    'city': city,
                    'year': int(year),
                    'month': int(month),
                    'anomalies': []
                }
            
            # Créer le message d'anomalie
            anomaly_msg = {
                'event_time': str(anomaly.get('event_time', '')) if anomaly.get('event_time') else None,
                'anomaly_type': str(anomaly.get('anomaly_type', 'unknown')),
                'observed_temperature': float(anomaly.get('observed_temperature')) if anomaly.get('observed_temperature') is not None else None,
                'expected_temperature': float(anomaly.get('expected_temperature')) if anomaly.get('expected_temperature') is not None else None,
                'observed_windspeed': float(anomaly.get('observed_windspeed')) if anomaly.get('observed_windspeed') is not None else None,
                'expected_windspeed': float(anomaly.get('expected_windspeed')) if anomaly.get('expected_windspeed') is not None else None,
                'temp_deviation': float(anomaly.get('temp_deviation')) if anomaly.get('temp_deviation') is not None else None,
                'wind_deviation': float(anomaly.get('wind_deviation')) if anomaly.get('wind_deviation') is not None else None,
            }
            
            city_year_month_anomalies[key]['anomalies'].append(anomaly_msg)
        
        # Sauvegarder chaque groupe
        for key, data in city_year_month_anomalies.items():
            country = data['country']
            city = data['city']
            year = data['year']
            month = data['month']
            
            country_clean = country.replace('/', '_').replace(' ', '_')
            city_clean = city.replace('/', '_').replace(' ', '_')
            
            hdfs_dir = f"{hdfs_base_path}/{country_clean}/{city_clean}/anomalies/{year}/{month:02d}"
            hdfs_file = f"{hdfs_dir}/anomalies.json"
            
            try:
                # Lire le fichier existant s'il existe
                existing_anomalies = []
                try:
                    result = subprocess.run(
                        ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-cat', hdfs_file],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    if result.returncode == 0:
                        existing_data = json.loads(result.stdout)
                        existing_anomalies = existing_data.get('anomalies', [])
                except:
                    pass
                
                # Ajouter les nouvelles anomalies
                existing_anomalies.extend(data['anomalies'])
                
                # Créer le fichier complet
                full_data = {
                    'country': country,
                    'city': city,
                    'year': year,
                    'month': month,
                    'anomalies': existing_anomalies,
                    'total_anomalies': len(existing_anomalies),
                    'updated_at': datetime.now().isoformat()
                }
                
                # Créer le répertoire
                subprocess.run(
                    ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', hdfs_dir],
                    check=True,
                    capture_output=True,
                    timeout=10
                )
                
                # Écrire dans un fichier temporaire
                temp_file = f"/tmp/anomalies_{country_clean}_{city_clean}_{year}_{month:02d}.json"
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(full_data, f, indent=2, ensure_ascii=False)
                
                # Copier dans le conteneur
                subprocess.run(
                    ['docker', 'cp', temp_file, f'namenode:/tmp/anomalies_temp.json'],
                    check=True,
                    capture_output=True,
                    timeout=10
                )
                
                # Déplacer dans HDFS
                subprocess.run(
                    ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', '-f', '/tmp/anomalies_temp.json', hdfs_file],
                    check=True,
                    capture_output=True,
                    timeout=10
                )
                
                # Nettoyer
                os.remove(temp_file)
                subprocess.run(
                    ['docker', 'exec', 'namenode', 'rm', '-f', '/tmp/anomalies_temp.json'],
                    capture_output=True
                )
                
                print(f"✅ {len(data['anomalies'])} anomalie(s) sauvegardée(s): {hdfs_file}")
                
            except Exception as e:
                print(f"⚠️  Erreur lors de la sauvegarde pour {city}, {country}: {e}", file=sys.stderr)
    
    return foreach_batch_function


def main():
    """Fonction principale."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Détecteur d\'anomalies climatiques en temps réel - Jointure Batch vs Speed',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  %(prog)s
  %(prog)s --kafka-servers kafka:9092
  %(prog)s --temp-threshold 5 --wind-threshold 2
        """
    )
    
    parser.add_argument(
        '--kafka-servers',
        default='kafka:9092',
        help='Serveurs Kafka bootstrap (défaut: kafka:9092)'
    )
    
    parser.add_argument(
        '--input-topic',
        default='weather_transformed',
        help='Topic Kafka source (défaut: weather_transformed)'
    )
    
    parser.add_argument(
        '--output-topic',
        default='weather_anomalies',
        help='Topic Kafka destination (défaut: weather_anomalies)'
    )
    
    parser.add_argument(
        '--hdfs-path',
        default='/hdfs-data',
        help='Chemin HDFS de base (défaut: /hdfs-data)'
    )
    
    parser.add_argument(
        '--temp-threshold',
        type=float,
        default=5.0,
        help='Seuil d\'anomalie température en °C (défaut: 5.0)'
    )
    
    parser.add_argument(
        '--wind-threshold',
        type=float,
        default=2.0,
        help='Seuil d\'anomalie vent en écarts-types (défaut: 2.0)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🔍 Détecteur d'anomalies climatiques en temps réel")
    print("=" * 60)
    print(f"📥 Topic source: {args.input_topic}")
    print(f"📤 Topic destination: {args.output_topic}")
    print(f"🌡️  Seuil température: ±{args.temp_threshold}°C")
    print(f"💨 Seuil vent: {args.wind_threshold} écarts-types")
    print("=" * 60)
    print()
    
    # Créer la session Spark
    spark = create_spark_session()
    
    try:
        # Charger les profils saisonniers depuis HDFS (batch layer)
        profiles_dict = load_seasonal_profiles_from_hdfs(args.hdfs_path)
        
        if not profiles_dict:
            print("❌ Aucun profil saisonnier disponible", file=sys.stderr)
            print("   Exécutez d'abord l'exercice 12 pour créer les profils enrichis", file=sys.stderr)
            sys.exit(1)
        
        # Créer un DataFrame broadcast avec les profils
        profiles_df = create_profiles_broadcast(spark, profiles_dict)
        
        # Lire depuis Kafka (speed layer)
        print(f"\n📡 Lecture en temps réel depuis Kafka: {args.input_topic}")
        
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.kafka_servers) \
            .option("subscribe", args.input_topic) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Définir le schéma pour weather_transformed
        schema = StructType([
            StructField("event_time", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("location_name", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("windspeed", DoubleType(), True),
            StructField("winddirection", DoubleType(), True),
            StructField("weathercode", IntegerType(), True),
            StructField("weather_time", StringType(), True),
            StructField("wind_alert_level", StringType(), True),
            StructField("heat_alert_level", StringType(), True),
            StructField("original_timestamp", StringType(), True),
            StructField("data_source", StringType(), True)
        ])
        
        # Parser le JSON
        weather_df = df.select(
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("value"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), schema).alias("data")
        ).select(
            "message_key",
            "kafka_timestamp",
            "data.*"
        )
        
        # Détecter les anomalies
        anomalies_df = detect_anomalies(weather_df, profiles_df, args.temp_threshold, args.wind_threshold)
        
        # Créer les messages d'anomalie
        anomaly_messages = create_anomaly_messages(anomalies_df)
        
        # Fonction pour traiter chaque batch
        def foreach_batch_kafka(batch_df, batch_id):
            """Traite chaque batch et affiche les statistiques."""
            count = batch_df.count()
            if count > 0:
                anomalies_count = batch_df.filter(col('is_anomaly') == True).count()
                print(f"\n📨 Batch {batch_id}: {count} message(s), {anomalies_count} anomalie(s) détectée(s)")
                
                if anomalies_count > 0:
                    print("🚨 Anomalies détectées:")
                    batch_df.filter(col('is_anomaly') == True).select(
                        'city', 'country', 'anomaly_type', 'temp_deviation', 'wind_deviation'
                    ).show(truncate=False)
        
        # Écrire dans Kafka
        query_kafka = anomaly_messages \
            .select(
                expr("concat(country, '/', city)").alias("key"),
                to_json(struct([
                    col('event_time'),
                    col('city'),
                    col('country'),
                    col('variable'),
                    col('observed_value'),
                    col('expected_value'),
                    col('anomaly_type')
                ])).alias("value")
            ) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.kafka_servers) \
            .option("topic", args.output_topic) \
            .option("checkpointLocation", "/tmp/checkpoint/anomaly_detector_kafka") \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .start()
        
        # Sauvegarder dans HDFS
        foreach_batch_hdfs = create_hdfs_saver(args.hdfs_path)
        query_hdfs = anomalies_df \
            .writeStream \
            .foreachBatch(foreach_batch_hdfs) \
            .outputMode("update") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        # Afficher les statistiques
        query_stats = anomalies_df \
            .writeStream \
            .foreachBatch(foreach_batch_kafka) \
            .outputMode("update") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print("✅ Détection d'anomalies démarrée!")
        print("📊 En attente de données...")
        print("   (Appuyez sur Ctrl+C pour arrêter)")
        print()
        
        # Attendre la terminaison
        query_kafka.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt du détecteur d'anomalies...")
        query_kafka.stop()
        query_hdfs.stop()
        query_stats.stop()
        print("✅ Détecteur arrêté.")
    except Exception as e:
        print(f"❌ Erreur: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
