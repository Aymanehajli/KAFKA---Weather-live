#!/bin/bash

# Script pour envoyer un message statique au topic weather_stream

echo "Envoi du message au topic weather_stream..."
echo '{"msg": "Hello Kafka"}' | docker exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic weather_stream

echo ""
echo "Message envoyé avec succès!"
