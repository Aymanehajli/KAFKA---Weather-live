#!/usr/bin/env python3
"""
Script Spark Streaming pour transformer les données météo et détecter les alertes - Exercice 4
Lit depuis weather_stream et écrit dans weather_transformed avec les niveaux d'alerte.
"""

import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, when, lit, current_timestamp, 
    struct, to_json, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, MapType
)


def create_spark_session(app_name="WeatherTransformer"):
    """Crée une session Spark avec les configurations nécessaires."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_weather_schema():
    """Définit le schéma pour les données météo."""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("location", StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("location_name", StringType(), True)
        ]), True),
        StructField("current_weather", StructType([
            StructField("temperature", DoubleType(), True),
            StructField("windspeed", DoubleType(), True),
            StructField("winddirection", DoubleType(), True),
            StructField("weathercode", IntegerType(), True),
            StructField("time", StringType(), True)
        ]), True),
        StructField("source", StringType(), True)
    ])


def calculate_wind_alert_level(windspeed_col):
    """Calcule le niveau d'alerte pour le vent."""
    return when(windspeed_col < 10, "level_0") \
        .when((windspeed_col >= 10) & (windspeed_col <= 20), "level_1") \
        .when(windspeed_col > 20, "level_2") \
        .otherwise("level_0")


def calculate_heat_alert_level(temperature_col):
    """Calcule le niveau d'alerte pour la chaleur."""
    return when(temperature_col < 25, "level_0") \
        .when((temperature_col >= 25) & (temperature_col <= 35), "level_1") \
        .when(temperature_col > 35, "level_2") \
        .otherwise("level_0")


def transform_weather_data(spark, kafka_bootstrap_servers, input_topic, output_topic):
    """Transforme les données météo et détecte les alertes."""
    
    print("=" * 60)
    print("🌤️  Démarrage du transformateur météo Spark")
    print("=" * 60)
    print(f"📥 Topic source: {input_topic}")
    print(f"📤 Topic destination: {output_topic}")
    print(f"🔗 Kafka: {kafka_bootstrap_servers}")
    print("=" * 60)
    print()
    
    # Lire depuis Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parser le JSON
    schema = define_weather_schema()
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
    
    # Extraire les champs et calculer les alertes
    transformed_df = weather_df.select(
        # Timestamp de l'événement
        current_timestamp().alias("event_time"),
        
        # Données de localisation
        col("location.latitude").alias("latitude"),
        col("location.longitude").alias("longitude"),
        col("location.city").alias("city"),
        col("location.country").alias("country"),
        col("location.location_name").alias("location_name"),
        
        # Données météo transformées
        col("current_weather.temperature").alias("temperature"),
        col("current_weather.windspeed").alias("windspeed"),
        col("current_weather.winddirection").alias("winddirection"),
        col("current_weather.weathercode").alias("weathercode"),
        col("current_weather.time").alias("weather_time"),
        
        # Niveaux d'alerte calculés
        calculate_wind_alert_level(col("current_weather.windspeed")).alias("wind_alert_level"),
        calculate_heat_alert_level(col("current_weather.temperature")).alias("heat_alert_level"),
        
        # Métadonnées
        col("timestamp").alias("original_timestamp"),
        col("source").alias("data_source"),
        col("message_key")
    )
    
    # Convertir en JSON pour l'écriture dans Kafka
    output_df = transformed_df.select(
        col("message_key").alias("key"),
        to_json(struct([
            col("event_time"),
            col("latitude"),
            col("longitude"),
            col("city"),
            col("country"),
            col("location_name"),
            col("temperature"),
            col("windspeed"),
            col("winddirection"),
            col("weathercode"),
            col("weather_time"),
            col("wind_alert_level"),
            col("heat_alert_level"),
            col("original_timestamp"),
            col("data_source")
        ])).alias("value")
    )
    
    # Fonction pour traiter chaque batch et afficher les statistiques
    def foreach_batch_function(batch_df, batch_id):
        """Traite chaque batch et affiche les statistiques."""
        count = batch_df.count()
        if count > 0:
            print(f"\n📨 Batch {batch_id}: {count} message(s) transformé(s) et envoyé(s) à {output_topic}")
            # Afficher un échantillon des clés (localisations)
            print("📍 Localisations traitées:")
            batch_df.select("key").distinct().show(truncate=False)
    
    # Utiliser un checkpoint fixe (sera nettoyé avant le démarrage si nécessaire)
    checkpoint_location = "/tmp/checkpoint/weather_transformer"
    
    print(f"💾 Checkpoint location: {checkpoint_location}")
    print()
    
    # Écrire dans Kafka
    query = output_df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    print("✅ Transformation démarrée!")
    print("📊 En attente de données...")
    print("   (Appuyez sur Ctrl+C pour arrêter)")
    print()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt du transformateur...")
        query.stop()
        print("✅ Transformateur arrêté.")


def main():
    """Fonction principale."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Transformateur météo Spark - Traite weather_stream et produit weather_transformed',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  %(prog)s
  %(prog)s --kafka-servers kafka:9092
  %(prog)s --input-topic weather_stream --output-topic weather_transformed
        """
    )
    
    parser.add_argument(
        '--kafka-servers',
        default='kafka:9092',
        help='Serveurs Kafka bootstrap (défaut: kafka:9092 pour Docker)'
    )
    
    parser.add_argument(
        '--input-topic',
        default='weather_stream',
        help='Topic source (défaut: weather_stream)'
    )
    
    parser.add_argument(
        '--output-topic',
        default='weather_transformed',
        help='Topic destination (défaut: weather_transformed)'
    )
    
    args = parser.parse_args()
    
    # Créer la session Spark
    spark = create_spark_session()
    
    try:
        # Lancer la transformation
        transform_weather_data(
            spark,
            args.kafka_servers,
            args.input_topic,
            args.output_topic
        )
    except Exception as e:
        print(f"❌ Erreur: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
