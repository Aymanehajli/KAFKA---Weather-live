#!/bin/bash

# Script pour créer le topic weather_history

echo "Création du topic weather_history..."

docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic weather_history \
  --partitions 1 \
  --replication-factor 1 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Topic weather_history créé avec succès!"
else
    echo "⚠️  Le topic existe peut-être déjà (c'est normal si vous relancez le script)"
fi

# Afficher la liste des topics pour vérification
echo ""
echo "Liste des topics existants:"
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
