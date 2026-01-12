#!/bin/bash
# Script pour lancer le détecteur d'anomalies climatiques

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🔍 Démarrage du détecteur d'anomalies climatiques"
echo "=================================================="
echo ""

# Vérifier que les conteneurs sont en cours d'exécution
echo "🔍 Vérification des conteneurs..."
if ! docker ps | grep -q "kafka"; then
    echo "❌ Le conteneur Kafka n'est pas en cours d'exécution"
    echo "   Lancez: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q "pyspark_notebook"; then
    echo "❌ Le conteneur pyspark_notebook n'est pas en cours d'exécution"
    echo "   Lancez: docker-compose up -d"
    exit 1
fi

# Créer le topic weather_anomalies si nécessaire
echo "📝 Vérification du topic weather_anomalies..."
if ! docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092 | grep -q "weather_anomalies"; then
    echo "   Création du topic..."
    ./create_anomalies_topic.sh
else
    echo "   ✅ Topic weather_anomalies existe déjà"
fi

# Nettoyer le checkpoint si nécessaire
echo ""
echo "🧹 Nettoyage des checkpoints précédents..."
docker exec pyspark_notebook rm -rf /tmp/checkpoint/anomaly_detector_kafka 2>/dev/null || true
docker exec pyspark_notebook rm -rf /tmp/checkpoint/anomaly_detector_hdfs 2>/dev/null || true
echo "   ✅ Checkpoints nettoyés"

# Copier le script dans le conteneur
echo ""
echo "📋 Copie du script dans le conteneur..."
docker cp anomaly_detector.py pyspark_notebook:/tmp/anomaly_detector.py

# Lancer le job Spark
echo ""
echo "🚀 Lancement du détecteur d'anomalies..."
echo "   (Appuyez sur Ctrl+C pour arrêter)"
echo ""

docker exec -it pyspark_notebook spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint/anomaly_detector_kafka \
  --conf spark.sql.streaming.schemaInference=true \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  /tmp/anomaly_detector.py \
  --kafka-servers kafka:9092 \
  --input-topic weather_transformed \
  --output-topic weather_anomalies \
  --hdfs-path /hdfs-data \
  --temp-threshold 5.0 \
  --wind-threshold 2.0

echo ""
echo "✅ Détecteur d'anomalies arrêté"
