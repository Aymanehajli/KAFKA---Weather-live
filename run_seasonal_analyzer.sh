#!/bin/bash

# Script pour exécuter l'analyseur de profils saisonniers

echo "=========================================="
echo "Démarrage de l'analyseur de profils saisonniers"
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

# Copier le script dans le conteneur
echo "📦 Copie du script dans le conteneur Spark..."
docker cp seasonal_profile_analyzer.py pyspark_notebook:/home/jovyan/work/seasonal_profile_analyzer.py

# Exécuter l'analyseur
echo "🚀 Démarrage de l'analyseur..."
echo ""

docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --master local[*] \
  /home/jovyan/work/seasonal_profile_analyzer.py \
  --hdfs-path /hdfs-data
