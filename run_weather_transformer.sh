#!/bin/bash

# Script pour exécuter le transformateur météo Spark

echo "=========================================="
echo "Démarrage du transformateur météo Spark"
echo "=========================================="
echo ""

# Vérifier que les services sont en cours d'exécution
if ! docker ps | grep -q kafka; then
    echo "❌ Erreur: Le conteneur Kafka n'est pas en cours d'exécution."
    echo "Veuillez démarrer les services avec: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q pyspark_notebook; then
    echo "❌ Erreur: Le conteneur pyspark_notebook n'est pas en cours d'exécution."
    echo "Veuillez démarrer les services avec: docker-compose up -d"
    exit 1
fi

# Créer le topic weather_transformed si nécessaire
echo "📋 Vérification du topic weather_transformed..."
./create_transformed_topic.sh
echo ""

# Nettoyer les anciens checkpoints pour éviter les conflits
echo "🧹 Nettoyage des anciens checkpoints..."
docker exec pyspark_notebook bash -c "rm -rf /tmp/checkpoint/weather_transformer*" 2>/dev/null
echo "✅ Checkpoints nettoyés"
echo ""

# Copier le script dans le conteneur
echo "📦 Copie du script dans le conteneur Spark..."
docker cp weather_transformer.py pyspark_notebook:/home/jovyan/work/weather_transformer.py

# Exécuter le transformateur dans le conteneur
echo "🚀 Démarrage du transformateur..."
echo "   (Appuyez sur Ctrl+C pour arrêter)"
echo ""

# Utiliser spark-submit pour exécuter le script PySpark
# Note: Le package Kafka sera téléchargé automatiquement au premier lancement
docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master local[*] \
  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint/weather_transformer \
  /home/jovyan/work/weather_transformer.py \
  --kafka-servers kafka:9092 \
  --input-topic weather_stream \
  --output-topic weather_transformed
