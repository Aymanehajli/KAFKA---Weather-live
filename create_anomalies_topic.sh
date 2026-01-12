#!/bin/bash
# Script pour créer le topic Kafka weather_anomalies

echo "📝 Création du topic Kafka: weather_anomalies"

docker exec kafka kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --topic weather_anomalies \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "✅ Topic weather_anomalies créé avec succès"

# Afficher les détails du topic
echo ""
echo "📊 Détails du topic:"
docker exec kafka kafka-topics.sh \
  --describe \
  --bootstrap-server localhost:9092 \
  --topic weather_anomalies
