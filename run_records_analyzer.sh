#!/bin/bash

# Script pour exécuter l'analyseur de records climatiques

echo "=========================================="
echo "Démarrage de l'analyseur de records climatiques"
echo "=========================================="
echo ""

# Vérifier que les services sont en cours d'exécution
if ! docker ps | grep -q namenode; then
    echo "❌ Erreur: Le conteneur namenode n'est pas en cours d'exécution."
    echo "Veuillez démarrer les services avec: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q pyspark_notebook; then
    echo "❌ Erreur: Le conteneur pyspark_notebook n'est pas en cours d'exécution."
    echo "Veuillez démarrer les services avec: docker-compose up -d"
    exit 1
fi

# Vérifier que des données historiques existent dans HDFS
echo "🔍 Vérification des données historiques dans HDFS..."
if ! docker exec namenode hdfs dfs -ls -R /hdfs-data | grep -q "weather_history_raw"; then
    echo "⚠️  Aucune donnée historique trouvée dans HDFS."
    echo "Veuillez d'abord exécuter l'exercice 9 pour télécharger les données historiques."
    echo "Exemple: python3 weather_history_loader.py --city Paris --country France"
    exit 1
fi

echo "✅ Données historiques trouvées"
echo ""

# Créer le topic Kafka si nécessaire
echo "📋 Vérification du topic weather_records..."
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic weather_records \
  --partitions 1 \
  --replication-factor 1 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Topic weather_records créé"
else
    echo "⚠️  Le topic existe peut-être déjà"
fi
echo ""

# Copier le script dans le conteneur
echo "📦 Copie du script dans le conteneur Spark..."
docker cp weather_records_analyzer.py pyspark_notebook:/home/jovyan/work/weather_records_analyzer.py

# Exécuter l'analyseur
echo "🚀 Démarrage de l'analyseur..."
echo ""

docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --master local[*] \
  /home/jovyan/work/weather_records_analyzer.py \
  --hdfs-path /hdfs-data \
  --kafka-servers kafka:9092 \
  --kafka-topic weather_records
