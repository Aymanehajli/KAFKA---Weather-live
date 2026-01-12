#!/bin/bash

# Script de test pour le producteur météo

echo "=========================================="
echo "Test du producteur météo Kafka - Exercice 3"
echo "=========================================="
echo ""

# Vérifier que Kafka est en cours d'exécution
if ! docker ps | grep -q kafka; then
    echo "❌ Erreur: Le conteneur Kafka n'est pas en cours d'exécution."
    echo "Veuillez démarrer les services avec: docker-compose up -d"
    exit 1
fi

# Vérifier que les dépendances sont installées
if ! python3 -c "import kafka, requests" 2>/dev/null; then
    echo "❌ Erreur: Les dépendances Python ne sont pas installées."
    echo "Installez-les avec: pip3 install -r requirements.txt"
    exit 1
fi

echo "✅ Kafka est en cours d'exécution"
echo "✅ Dépendances Python installées"
echo ""

# Coordonnées par défaut (Paris)
LAT=${1:-48.8566}
LON=${2:-2.3522}

echo "📍 Localisation: ($LAT, $LON)"
echo ""

# Lancer le producteur
echo "🚀 Lancement du producteur météo..."
echo ""

python3 current_weather.py "$LAT" "$LON"

echo ""
echo "=========================================="
echo "Test terminé!"
echo "=========================================="
echo ""
echo "Pour vérifier les messages, utilisez:"
echo "  python3 kafka_consumer.py weather_stream --from-beginning"
