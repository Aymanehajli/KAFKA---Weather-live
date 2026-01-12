#!/usr/bin/env python3
"""
Script Spark Streaming pour calculer des agrégats en temps réel - Exercice 5
Lit depuis weather_transformed et calcule des métriques sur des fenêtres glissantes.
"""

import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, min as spark_min, max as spark_max,
    sum as spark_sum, when, struct, to_json, current_timestamp,
    expr, collect_list
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)


def create_spark_session(app_name="WeatherAggregator"):
    """Crée une session Spark avec les configurations nécessaires."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_transformed_schema():
    """Définit le schéma pour les données transformées."""
    return StructType([
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


def calculate_aggregates(spark, kafka_bootstrap_servers, input_topic, window_duration="5 minutes", slide_duration="1 minute"):
    """Calcule les agrégats sur des fenêtres glissantes."""
    
    print("=" * 60)
    print("📊 Démarrage de l'agrégateur météo Spark")
    print("=" * 60)
    print(f"📥 Topic source: {input_topic}")
    print(f"⏱️  Fenêtre: {window_duration}")
    print(f"🔄 Glissement: {slide_duration}")
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
    schema = define_transformed_schema()
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
    
    # Convertir event_time en timestamp
    weather_df = weather_df.withColumn(
        "event_timestamp",
        col("event_time").cast("timestamp")
    )
    
    # Créer une clé de localisation (latitude,longitude)
    weather_df = weather_df.withColumn(
        "location_key",
        expr("concat(cast(latitude as string), ',', cast(longitude as string))")
    )
    
    # Créer une clé de localisation améliorée (ville/pays si disponible, sinon coordonnées)
    weather_df = weather_df.withColumn(
        "location_key",
        when(
            col("city").isNotNull() & col("country").isNotNull(),
            expr("concat(country, '/', city)")
        ).otherwise(
            expr("concat(cast(latitude as string), ',', cast(longitude as string))")
        )
    )
    
    # Appliquer la fenêtre glissante
    windowed_df = weather_df \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), window_duration, slide_duration),
            col("location_key"),
            col("city"),
            col("country")
        ) \
        .agg(
            # Métriques de température
            avg("temperature").alias("avg_temperature"),
            spark_min("temperature").alias("min_temperature"),
            spark_max("temperature").alias("max_temperature"),
            
            # Nombre d'alertes par type
            spark_sum(
                when(col("wind_alert_level").isin(["level_1", "level_2"]), 1).otherwise(0)
            ).alias("wind_alerts_count"),
            spark_sum(
                when(col("heat_alert_level").isin(["level_1", "level_2"]), 1).otherwise(0)
            ).alias("heat_alerts_count"),
            
            # Détail des alertes
            spark_sum(
                when(col("wind_alert_level") == "level_1", 1).otherwise(0)
            ).alias("wind_level_1_count"),
            spark_sum(
                when(col("wind_alert_level") == "level_2", 1).otherwise(0)
            ).alias("wind_level_2_count"),
            spark_sum(
                when(col("heat_alert_level") == "level_1", 1).otherwise(0)
            ).alias("heat_level_1_count"),
            spark_sum(
                when(col("heat_alert_level") == "level_2", 1).otherwise(0)
            ).alias("heat_level_2_count"),
            
            # Nombre total d'alertes
            spark_sum(
                when(
                    col("wind_alert_level").isin(["level_1", "level_2"]) |
                    col("heat_alert_level").isin(["level_1", "level_2"]),
                    1
                ).otherwise(0)
            ).alias("total_alerts_count"),
            
            # Nombre total de messages
            count("*").alias("total_messages"),
            
            # Coordonnées (pour référence)
            avg("latitude").alias("latitude"),
            avg("longitude").alias("longitude"),
            
            # Première ville et pays (pour référence)
            expr("first(city)").alias("city"),
            expr("first(country)").alias("country")
        )
    
    # Ajouter des colonnes calculées
    aggregated_df = windowed_df \
        .withColumn(
            "window_start",
            col("window.start")
        ) \
        .withColumn(
            "window_end",
            col("window.end")
        ) \
        .select(
            col("window_start").alias("window_start"),
            col("window_end").alias("window_end"),
            col("location_key").alias("location"),
            col("city"),
            col("country"),
            col("latitude"),
            col("longitude"),
            col("avg_temperature"),
            col("min_temperature"),
            col("max_temperature"),
            col("wind_alerts_count"),
            col("heat_alerts_count"),
            col("wind_level_1_count"),
            col("wind_level_2_count"),
            col("heat_level_1_count"),
            col("heat_level_2_count"),
            col("total_alerts_count"),
            col("total_messages"),
            current_timestamp().alias("computed_at")
        )
    
    # Fonction pour afficher les agrégats
    def foreach_batch_function(batch_df, batch_id):
        """Affiche les agrégats calculés."""
        count_rows = batch_df.count()
        if count_rows > 0:
            print(f"\n{'='*60}")
            print(f"📊 Batch {batch_id}: {count_rows} agrégat(s) calculé(s)")
            print(f"{'='*60}")
            
            # Afficher les résultats
            batch_df.show(truncate=False)
            
            # Afficher un résumé
            print("\n📈 Résumé des alertes:")
            batch_df.select(
                col("location"),
                col("wind_alerts_count"),
                col("heat_alerts_count"),
                col("total_alerts_count")
            ).show(truncate=False)
            
            print("\n🌡️  Résumé des températures:")
            batch_df.select(
                col("location"),
                col("avg_temperature"),
                col("min_temperature"),
                col("max_temperature")
            ).show(truncate=False)
    
    # Écrire les résultats dans la console (ou dans un topic Kafka si nécessaire)
    query = aggregated_df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .outputMode("update") \
        .trigger(processingTime=slide_duration) \
        .start()
    
    print("✅ Agrégation démarrée!")
    print("📊 Calcul des métriques en temps réel...")
    print("   (Appuyez sur Ctrl+C pour arrêter)")
    print()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt de l'agrégateur...")
        query.stop()
        print("✅ Agrégateur arrêté.")


def main():
    """Fonction principale."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Agrégateur météo Spark - Calcule des agrégats en temps réel sur weather_transformed',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  %(prog)s
  %(prog)s --window 1 minute --slide 30 seconds
  %(prog)s --window 5 minutes --slide 1 minute
        """
    )
    
    parser.add_argument(
        '--kafka-servers',
        default='kafka:9092',
        help='Serveurs Kafka bootstrap (défaut: kafka:9092 pour Docker)'
    )
    
    parser.add_argument(
        '--input-topic',
        default='weather_transformed',
        help='Topic source (défaut: weather_transformed)'
    )
    
    parser.add_argument(
        '--window',
        default='5 minutes',
        help='Durée de la fenêtre (défaut: 5 minutes)'
    )
    
    parser.add_argument(
        '--slide',
        default='1 minute',
        help='Durée du glissement (défaut: 1 minute)'
    )
    
    args = parser.parse_args()
    
    # Créer la session Spark
    spark = create_spark_session()
    
    try:
        # Lancer l'agrégation
        calculate_aggregates(
            spark,
            args.kafka_servers,
            args.input_topic,
            args.window,
            args.slide
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
