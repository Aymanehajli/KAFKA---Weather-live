#!/bin/bash
# Script pour lancer le dashboard météo global

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🌐 Démarrage du Dashboard Météo Global"
echo "======================================"
echo ""

# Vérifier que les conteneurs sont en cours d'exécution
echo "🔍 Vérification des conteneurs..."
if ! docker ps | grep -q "kafka"; then
    echo "❌ Le conteneur Kafka n'est pas en cours d'exécution"
    echo "   Lancez: docker-compose up -d"
    exit 1
fi

if ! docker ps | grep -q "namenode"; then
    echo "❌ Le conteneur namenode n'est pas en cours d'exécution"
    echo "   Lancez: docker-compose up -d"
    exit 1
fi

# Vérifier les dépendances Python
echo ""
echo "📦 Vérification des dépendances..."
if ! python3 -c "import flask" 2>/dev/null; then
    echo "   Installation de Flask..."
    pip3 install flask flask-cors --quiet
fi

# Lancer le serveur
echo ""
echo "🚀 Démarrage du serveur..."
echo "   Dashboard disponible sur: http://localhost:5001"
echo "   API disponible sur: http://localhost:5001/api"
echo ""
echo "   (Appuyez sur Ctrl+C pour arrêter)"
echo ""

python3 weather_dashboard_api.py
