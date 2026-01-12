#!/bin/bash

# Script pour créer le topic Kafka weather_stream

echo "Création du topic weather_stream..."

docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic weather_stream \
  --partitions 1 \
  --replication-factor 1

echo "Topic weather_stream créé avec succès!"

# Afficher la liste des topics pour vérification
echo ""
echo "Liste des topics existants:"
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
