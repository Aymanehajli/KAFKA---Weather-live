#!/bin/bash

# Script pour envoyer un message de test au topic weather_stream

TOPIC=${1:-weather_stream}
MESSAGE=${2:-'{"msg": "Test message", "timestamp": "'$(date +%s)'", "source": "test_script"}'}

echo "📤 Envoi d'un message au topic: $TOPIC"
echo "Message: $MESSAGE"
echo ""

echo "$MESSAGE" | docker exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC"

echo ""
echo "✅ Message envoyé avec succès!"
