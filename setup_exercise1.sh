#!/bin/bash

# Script complet pour l'exercice 1 : Création du topic et envoi du message

echo "=========================================="
echo "Exercice 1 : Mise en place de Kafka"
echo "=========================================="
echo ""

# Vérifier que Kafka est en cours d'exécution
if ! docker ps | grep -q kafka; then
    echo "Erreur: Le conteneur Kafka n'est pas en cours d'exécution."
    echo "Veuillez démarrer les services avec: docker-compose up -d"
    exit 1
fi

echo "1. Création du topic weather_stream..."
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic weather_stream \
  --partitions 1 \
  --replication-factor 1 2>/dev/null

if [ $? -eq 0 ]; then
    echo "   ✓ Topic weather_stream créé avec succès!"
else
    echo "   ⚠ Le topic existe peut-être déjà (c'est normal si vous relancez le script)"
fi

echo ""
echo "2. Envoi du message statique..."
echo '{"msg": "Hello Kafka"}' | docker exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic weather_stream

if [ $? -eq 0 ]; then
    echo "   ✓ Message envoyé avec succès!"
else
    echo "   ✗ Erreur lors de l'envoi du message"
    exit 1
fi

echo ""
echo "=========================================="
echo "Exercice 1 terminé avec succès!"
echo "=========================================="
echo ""
echo "Pour vérifier que le message a été reçu, vous pouvez utiliser:"
echo "  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_stream --from-beginning"
