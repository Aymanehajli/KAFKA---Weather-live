#!/bin/bash

# Script pour nettoyer les checkpoints Spark

echo "🧹 Nettoyage des checkpoints Spark..."

docker exec pyspark_notebook bash -c "rm -rf /tmp/checkpoint/weather_transformer*"

if [ $? -eq 0 ]; then
    echo "✅ Checkpoints nettoyés avec succès!"
else
    echo "⚠️  Aucun checkpoint à nettoyer ou erreur lors du nettoyage"
fi
