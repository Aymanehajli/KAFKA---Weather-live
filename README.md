# 🌤️ Kafka Weather Live Dashboard

Système complet de traitement et visualisation de données météorologiques en temps réel utilisant Kafka, Spark Streaming, HDFS et un dashboard web interactif.

## 📋 Description

Ce projet implémente un pipeline de données météorologiques complet qui :
- **Ingère** des données météo en temps réel via Kafka
- **Transforme** et **détecte des alertes** avec Spark Streaming
- **Agrège** les données en temps réel
- **Stocke** les données historiques dans HDFS
- **Analyse** les profils saisonniers et détecte les anomalies
- **Visualise** toutes les données via un dashboard web interactif

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Open-Meteo │────▶│   Kafka     │────▶│    Spark    │
│     API     │     │  (Producer) │     │  Streaming  │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                                 │
                                                 ▼
                                          ┌─────────────┐
                                          │    HDFS     │
                                          │  (Storage)  │
                                          └──────┬──────┘
                                                 │
                                                 ▼
                                          ┌─────────────┐
                                          │  Dashboard  │
                                          │    Web      │
                                          └─────────────┘
```

## 🚀 Démarrage Rapide

### Prérequis

- Docker et Docker Compose
- Python 3.8+
- Git

### Installation

1. **Cloner le dépôt** :
```bash
git clone https://github.com/Aymanehajli/KAFKA---Weather-live.git
cd KAFKA---Weather-live
```

2. **Démarrer l'infrastructure** :
```bash
docker-compose up -d
```

3. **Installer les dépendances Python** :
```bash
pip3 install -r requirements.txt
```

4. **Créer les topics Kafka** :
```bash
./create_topic.sh
./create_transformed_topic.sh
./create_anomalies_topic.sh
```

5. **Lancer le dashboard** :
```bash
./run_dashboard.sh
```

Le dashboard sera accessible sur : **http://localhost:5001**

## 📚 Exercices Implémentés

### Exercice 1 : Mise en place de Kafka
- Création de topics Kafka
- Envoi de messages statiques

### Exercice 2 : Consommateur Kafka
- Script Python pour consommer les messages
- Affichage en temps réel

### Exercice 3 : Streaming de données météo
- Producteur qui interroge l'API Open-Meteo
- Envoi des données dans Kafka

### Exercice 4 : Transformation et détection d'alertes
- Spark Streaming pour transformer les données
- Calcul des niveaux d'alerte (vent et chaleur)
- Topic `weather_transformed`

### Exercice 5 : Agrégats en temps réel
- Fenêtres glissantes avec Spark
- Calcul de métriques agrégées
- Statistiques par ville/pays

### Exercice 6 : Extension du producteur
- Géocodage avec Open-Meteo
- Enrichissement avec ville/pays
- Partitionnement HDFS

### Exercice 7 : Stockage HDFS organisé
- Sauvegarde des alertes dans HDFS
- Structure : `/hdfs-data/{country}/{city}/alerts.json`

### Exercice 8 : Visualisation
- Graphiques d'évolution température/vent
- Répartition des alertes
- Codes météo les plus fréquents

### Exercice 9 : Séries historiques
- Téléchargement de 10 ans de données
- Stockage dans Kafka et HDFS

### Exercice 10 : Détection de records
- Job Spark pour trouver les records climatiques
- Jour le plus chaud/froid, vent le plus fort, etc.

### Exercice 11 : Profils saisonniers
- Analyse mensuelle des données historiques
- Calcul de moyennes et probabilités

### Exercice 12 : Validation et enrichissement
- Validation des profils saisonniers
- Calcul de statistiques de dispersion
- Quantiles et médianes

### Exercice 13 : Détection d'anomalies
- Jointure Batch vs Speed
- Détection d'anomalies en temps réel
- Publication dans Kafka et sauvegarde HDFS

### Exercice 13 Frontend : Dashboard Global
- Interface web complète
- Visualisations temps réel et historiques
- Tous les dashboards regroupés

## 📁 Structure du Projet

```
.
├── dashboard/                    # Interface web
│   ├── index.html
│   └── static/
│       ├── css/
│       └── js/
├── docker-compose.yml           # Configuration Docker
├── requirements.txt             # Dépendances Python
├── *.py                         # Scripts Python
├── *.sh                         # Scripts shell
└── README_EXERCICE*.md          # Documentation des exercices
```

## 🔧 Technologies Utilisées

- **Kafka** : Messagerie en temps réel
- **Spark Streaming** : Traitement de flux
- **HDFS** : Stockage distribué
- **Flask** : API backend
- **Chart.js** : Visualisations
- **Docker** : Orchestration
- **Open-Meteo API** : Données météo

## 📖 Documentation

Chaque exercice possède sa propre documentation :
- `README_EXERCICE1.md` à `README_EXERCICE13.md`
- `README_EXERCICE13_FRONTEND.md` pour le dashboard

## 🎯 Utilisation

### Lancer le producteur météo
```bash
python3 current_weather.py --city Paris --country France
```

### Lancer le transformateur Spark
```bash
./run_weather_transformer.sh
```

### Lancer l'agrégateur Spark
```bash
./run_weather_aggregator.sh
```

### Lancer le détecteur d'anomalies
```bash
./run_anomaly_detector.sh
```

### Consulter le dashboard
```bash
./run_dashboard.sh
# Ouvrir http://localhost:5001
```

## 📊 Fonctionnalités du Dashboard

- **Temps Réel** : Graphiques de température et vent en direct
- **Historique** : Données sur 10 ans
- **Profils Saisonniers** : Statistiques mensuelles
- **Anomalies** : Détection d'anomalies climatiques
- **Records** : Records climatiques locaux
- **Alertes** : Alertes météorologiques

## 🤝 Contribution

Les contributions sont les bienvenues ! N'hésitez pas à ouvrir une issue ou une pull request.

## 📝 Licence

Ce projet est un projet éducatif.

## 👤 Auteur

**Aymane Hajli**
- GitHub: [@Aymanehajli](https://github.com/Aymanehajli)

## 🙏 Remerciements

- Open-Meteo pour l'API météo gratuite
- Apache Kafka, Spark et Hadoop pour les outils Big Data

---

⭐ Si ce projet vous a été utile, n'hésitez pas à lui donner une étoile !
