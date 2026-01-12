#!/bin/bash

# Script pour exécuter l'agrégateur météo Spark

echo "=========================================="
echo "Démarrage de l'agrégateur météo Spark"
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

# Vérifier que le topic weather_transformed existe
if ! docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -q weather_transformed; then
    echo "⚠️  Le topic weather_transformed n'existe pas."
    echo "Veuillez d'abord exécuter l'exercice 4 pour créer ce topic."
    exit 1
fi

# Nettoyer les anciens checkpoints pour éviter les conflits
echo "🧹 Nettoyage des anciens checkpoints..."
docker exec pyspark_notebook bash -c "rm -rf /tmp/checkpoint/weather_aggregator*" 2>/dev/null
echo "✅ Checkpoints nettoyés"
echo ""

# Copier le script dans le conteneur
echo "📦 Copie du script dans le conteneur Spark..."
docker cp weather_aggregator.py pyspark_notebook:/home/jovyan/work/weather_aggregator.py

# Exécuter l'agrégateur dans le conteneur
echo "🚀 Démarrage de l'agrégateur..."
echo "   Fenêtre: 5 minutes, Glissement: 1 minute"
echo "   (Appuyez sur Ctrl+C pour arrêter)"
echo ""

# Utiliser spark-submit pour exécuter le script PySpark
docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master local[*] \
  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint/weather_aggregator \
  /home/jovyan/work/weather_aggregator.py \
  --kafka-servers kafka:9092 \
  --input-topic weather_transformed \
  --window "5 minutes" \
  --slide "1 minute"
