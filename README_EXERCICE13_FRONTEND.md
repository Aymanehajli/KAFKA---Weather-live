# Exercice 13 : Frontend Global de Visualisation Météo

## 📋 Objectif

Créer une interface web complète qui regroupe tous les dashboards et visualisations issus des exercices précédents :
- **Temps réel** : Données météo en streaming
- **Historique** : Données sur 10 ans
- **Profils saisonniers** : Statistiques mensuelles enrichies
- **Anomalies** : Détection d'anomalies climatiques
- **Records** : Records climatiques locaux
- **Alertes** : Alertes météorologiques

## 🎯 Fonctionnalités

### 1. Dashboard Temps Réel

- **Graphiques en temps réel** :
  - Évolution de la température
  - Évolution de la vitesse du vent
- **Statistiques** :
  - Nombre de messages reçus
  - Nombre d'anomalies détectées
  - Nombre d'agrégats calculés
- **Tableau des dernières données** :
  - Ville, température, vent, alertes, heure

### 2. Dashboard Historique

- **Graphiques sur 10 ans** :
  - Évolution de la température
  - Évolution de la vitesse du vent
- **Données depuis HDFS** :
  - Fichiers `weather_history_raw`

### 3. Profils Saisonniers

- **Graphiques mensuels** :
  - Température moyenne par mois
  - Vitesse du vent moyenne par mois
  - Probabilité d'alerte par mois
- **Statistiques** :
  - Mois le plus chaud
  - Mois le plus froid
  - Mois le plus venteux
- **Données depuis HDFS** :
  - Fichiers `seasonal_profile_enriched/{year}/profile.json`

### 4. Dashboard Anomalies

- **Statistiques** :
  - Total d'anomalies détectées
  - Répartition par type (vague de chaleur, vague de froid, tempête de vent)
- **Graphique en camembert** :
  - Répartition des anomalies par type
- **Tableau des dernières anomalies** :
  - Date, type, variable, valeur observée, valeur attendue, écart
- **Données depuis** :
  - Kafka (temps réel)
  - HDFS (`/anomalies/{year}/{month}/anomalies.json`)

### 5. Records Climatiques

- **Cartes de records** :
  - 🌡️ Jour le plus chaud de la décennie
  - ❄️ Jour le plus froid de la décennie
  - 💨 Rafale de vent la plus forte
  - 🌧️ Jour le plus pluvieux
- **Données depuis HDFS** :
  - Fichiers `weather_records/records.json`

### 6. Dashboard Alertes

- **Statistiques** :
  - Total d'alertes
  - Alertes vent (Level 1 et 2)
  - Alertes chaleur (Level 1 et 2)
- **Graphique en barres** :
  - Évolution des alertes par niveau
- **Tableau des dernières alertes** :
  - Date, ville, type, niveau, température, vent
- **Données depuis HDFS** :
  - Fichiers `alerts.json`

## 📁 Structure des Fichiers

```
dashboard/
├── index.html              # Page principale
├── static/
│   ├── css/
│   │   └── style.css      # Styles CSS
│   └── js/
│       └── dashboard.js   # Logique JavaScript
weather_dashboard_api.py    # API Backend Flask
run_dashboard.sh           # Script de lancement
```

## 🚀 Utilisation

### Prérequis

1. **Conteneurs Docker actifs** :
   ```bash
   docker-compose up -d
   ```

2. **Données disponibles** :
   - Topic Kafka `weather_transformed` actif
   - Données historiques dans HDFS (Exercice 9)
   - Profils saisonniers enrichis (Exercice 12)
   - Anomalies détectées (Exercice 13 - Détection)

3. **Dépendances Python** :
   ```bash
   pip3 install flask flask-cors
   ```

### Démarrage

```bash
# Lancer le dashboard
./run_dashboard.sh
```

Le dashboard sera accessible sur : **http://localhost:5001**

### Utilisation de l'Interface

1. **Sélectionner une ville** :
   - Choisir un pays dans le menu déroulant
   - Choisir une ville
   - Cliquer sur "Charger"

2. **Naviguer entre les onglets** :
   - **Temps Réel** : Données en streaming
   - **Historique** : Données sur 10 ans
   - **Profils Saisonniers** : Statistiques mensuelles
   - **Anomalies** : Anomalies détectées
   - **Records** : Records climatiques
   - **Alertes** : Alertes météorologiques

3. **Mises à jour automatiques** :
   - Les données temps réel se mettent à jour toutes les 5 secondes
   - L'indicateur de connexion montre l'état de Kafka

## 🔧 Architecture

### Backend API (Flask)

**Endpoints disponibles** :

- `GET /api/health` : État de santé de l'API
- `GET /api/realtime/weather` : Données météo temps réel
- `GET /api/realtime/anomalies` : Anomalies temps réel
- `GET /api/realtime/aggregates` : Agrégats temps réel
- `GET /api/historical/<country>/<city>` : Données historiques
- `GET /api/alerts/<country>/<city>` : Alertes
- `GET /api/records/<country>/<city>` : Records
- `GET /api/seasonal-profile/<country>/<city>` : Profil saisonnier
- `GET /api/anomalies/<country>/<city>` : Anomalies sauvegardées
- `GET /api/cities` : Liste des villes disponibles
- `GET /api/stats/summary` : Statistiques globales

### Frontend (HTML/CSS/JavaScript)

- **Framework** : Vanilla JavaScript
- **Graphiques** : Chart.js
- **Design** : Interface moderne et responsive
- **Mises à jour** : Polling toutes les 5 secondes

### Flux de Données

```
┌─────────────┐
│   Kafka     │  ← Données temps réel
│  (Topics)   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  API Flask  │  ← Consommation Kafka + Lecture HDFS
│  (Backend)  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Dashboard  │  ← Visualisations
│  (Frontend) │
└─────────────┘

┌─────────────┐
│    HDFS     │  ← Données historiques
│  (Storage)  │
└──────┬──────┘
       │
       └──────────→ API Flask
```

## 📊 Visualisations

### Graphiques Temps Réel

- **Ligne de température** : Évolution en temps réel
- **Ligne de vent** : Évolution en temps réel
- **Mise à jour** : Toutes les 5 secondes

### Graphiques Historiques

- **Ligne de température** : 10 ans de données
- **Ligne de vent** : 10 ans de données
- **Échantillonnage** : Optimisé pour performance

### Graphiques Saisonniers

- **Barres de température** : Moyenne par mois
- **Barres de vent** : Moyenne par mois
- **Ligne d'alertes** : Probabilité par mois

### Graphiques Anomalies

- **Camembert** : Répartition par type
- **Statistiques** : Compteurs par type

### Graphiques Alertes

- **Barres groupées** : Alertes par niveau et type
- **Statistiques** : Compteurs par niveau

## 🎨 Design

### Couleurs

- **Dégradé principal** : Violet/bleu (`#667eea` → `#764ba2`)
- **Température** : Rouge (`#ef4444`)
- **Vent** : Bleu (`#3b82f6`)
- **Alertes** : Orange (`#f59e0b`)
- **Anomalies** : Rouge (`#ef4444`)

### Responsive

- **Desktop** : Grille multi-colonnes
- **Tablet** : Adaptation automatique
- **Mobile** : Colonne unique

## 🔍 Dépannage

### Le dashboard ne se charge pas

**Vérifications** :
1. Le serveur Flask est-il lancé ?
2. Les conteneurs Docker sont-ils actifs ?
3. Le port 5001 est-il disponible ?

**Solution** :
```bash
# Vérifier les conteneurs
docker ps

# Vérifier le port
lsof -i :5001

# Relancer le dashboard
./run_dashboard.sh
```

### Aucune donnée temps réel

**Vérifications** :
1. Kafka est-il connecté ? (Vérifier l'indicateur de connexion)
2. Les topics existent-ils ?
3. Les producteurs sont-ils actifs ?

**Solution** :
```bash
# Vérifier les topics
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Vérifier les messages
python3 kafka_consumer.py weather_transformed
```

### Aucune ville disponible

**Vérifications** :
1. Les données historiques sont-elles dans HDFS ?
2. La structure HDFS est-elle correcte ?

**Solution** :
```bash
# Vérifier HDFS
docker exec namenode hdfs dfs -ls -R /hdfs-data

# Télécharger des données si nécessaire
python3 weather_history_loader.py --city Paris --country France --years 10
```

### Erreurs CORS

**Solution** :
- Flask-CORS est installé et configuré dans `weather_dashboard_api.py`
- Vérifier que `CORS(app)` est présent

### Graphiques vides

**Vérifications** :
1. Les données sont-elles chargées ? (Vérifier la console du navigateur)
2. La ville est-elle sélectionnée ?
3. Les données existent-elles pour cette ville ?

**Solution** :
- Ouvrir la console du navigateur (F12)
- Vérifier les erreurs JavaScript
- Vérifier les appels API dans l'onglet Network

## 📈 Performance

### Optimisations

- **Échantillonnage** : Les données historiques sont échantillonnées pour les graphiques
- **Cache** : Les données temps réel sont mises en cache
- **Polling** : Mises à jour toutes les 5 secondes (configurable)
- **Lazy Loading** : Les données sont chargées uniquement quand nécessaire

### Limites

- **Cache temps réel** : Maximum 1000 messages par topic
- **Échantillonnage historique** : Maximum 1000 points par graphique
- **Tableaux** : Affichage des 10-20 dernières entrées

## 🔐 Sécurité

- **CORS** : Activé pour le développement local
- **Validation** : Validation des paramètres d'entrée
- **Gestion d'erreurs** : Gestion robuste des erreurs

## 📚 Technologies Utilisées

- **Backend** :
  - Flask 3.0.0
  - Flask-CORS 4.0.0
  - kafka-python 2.0.2
- **Frontend** :
  - HTML5
  - CSS3 (Gradients, Flexbox, Grid)
  - JavaScript (ES6+)
  - Chart.js 4.4.0
- **Infrastructure** :
  - Docker
  - Kafka
  - HDFS

## ✅ Checklist de Validation

- [ ] Dashboard accessible sur http://localhost:5001
- [ ] API répond aux requêtes
- [ ] Données temps réel s'affichent
- [ ] Graphiques se mettent à jour automatiquement
- [ ] Sélection de ville fonctionne
- [ ] Tous les onglets affichent des données
- [ ] Indicateur de connexion Kafka fonctionne
- [ ] Responsive sur mobile/tablet

## 🎓 Fonctionnalités Avancées

### Extensions Possibles

1. **WebSockets** : Remplacement du polling par WebSockets pour des mises à jour en temps réel
2. **Filtres temporels** : Sélection de plages de dates
3. **Comparaisons** : Comparaison entre plusieurs villes
4. **Export** : Export des données en CSV/JSON
5. **Alertes personnalisées** : Configuration de seuils personnalisés
6. **Notifications** : Notifications push pour les anomalies critiques

---

**Exercice 13 Frontend terminé !** 🎉

Le dashboard global est opérationnel et regroupe toutes les visualisations des exercices précédents dans une interface web moderne et intuitive.
