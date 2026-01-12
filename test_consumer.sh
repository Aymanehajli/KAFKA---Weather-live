#!/bin/bash

# Script de test pour le consommateur Kafka

echo "=========================================="
echo "Test du consommateur Kafka - Exercice 2"
echo "=========================================="
echo ""

# Vérifier que Kafka est en cours d'exécution
if ! docker ps | grep -q kafka; then
    echo "❌ Erreur: Le conteneur Kafka n'est pas en cours d'exécution."
    echo "Veuillez démarrer les services avec: docker-compose up -d"
    exit 1
fi

# Vérifier que kafka-python est installé
if ! python3 -c "import kafka" 2>/dev/null; then
    echo "❌ Erreur: kafka-python n'est pas installé."
    echo "Installez-le avec: pip3 install -r requirements.txt"
    exit 1
fi

echo "✅ Kafka est en cours d'exécution"
echo "✅ kafka-python est installé"
echo ""

# Envoyer un message de test
echo "📤 Envoi d'un message de test au topic weather_stream..."
echo '{"msg": "Test consommateur Python", "test": true, "timestamp": "'$(date +%s)'"}' | \
  docker exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic weather_stream

echo ""
echo "✅ Message envoyé!"
echo ""
echo "=========================================="
echo "Démarrage du consommateur..."
echo "Appuyez sur Ctrl+C pour arrêter"
echo "=========================================="
echo ""

# Lancer le consommateur
python3 kafka_consumer.py weather_stream --from-beginning
