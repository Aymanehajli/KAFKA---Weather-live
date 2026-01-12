# Exercice 9 : Récupération de séries historiques longues

## Description

Cet exercice consiste à télécharger des données météo historiques sur 10 ans (ou une période personnalisée) pour une ville donnée via l'API archive d'Open-Meteo, puis à les stocker dans Kafka et HDFS.

## Prérequis

1. **Services Docker démarrés** (Kafka, HDFS namenode/datanode)
2. **Topic Kafka `weather_history`** créé (script fourni)
3. **Connexion Internet** pour accéder à l'API Open-Meteo Archive

## API Open-Meteo Archive

L'API archive d'Open-Meteo fournit des données météo historiques gratuites :
- **URL** : `https://archive-api.open-meteo.com/v1/archive`
- **Gratuite** : Pas de clé API requise
- **Données disponibles** : Jusqu'à plusieurs décennies selon la localisation
- **Format** : JSON avec données quotidiennes

## Installation et configuration

### 1. Créer le topic Kafka

```bash
./create_history_topic.sh
```

Ou manuellement :

```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic weather_history \
  --partitions 1 \
  --replication-factor 1
```

## Utilisation

### Syntaxe de base

```bash
python3 weather_history_loader.py --city <ville> --country <pays>
```

### Exemples

#### 1. Télécharger 10 ans de données pour Paris

```bash
python3 weather_history_loader.py --city Paris --country France
```

#### 2. Télécharger 5 ans de données pour New York

```bash
python3 weather_history_loader.py --city "New York" --country USA --years 5
```

#### 3. Télécharger avec coordonnées directes

```bash
python3 weather_history_loader.py --lat 48.8566 --lon 2.3522 --city Paris --country France
```

#### 4. Sauvegarder uniquement dans HDFS (pas Kafka)

```bash
python3 weather_history_loader.py --city Paris --country France --skip-kafka
```

#### 5. Envoyer uniquement à Kafka (pas HDFS)

```bash
python3 weather_history_loader.py --city Paris --country France --skip-hdfs
```

## Options disponibles

### Arguments de localisation

- `--city` : Nom de la ville (avec géocodage automatique)
- `--country` : Nom du pays (recommandé pour plus de précision)
- `--lat` / `--latitude` : Latitude (mode coordonnées)
- `--lon` / `--longitude` : Longitude (mode coordonnées)

### Arguments de configuration

- `--years` : Nombre d'années de données (défaut: 10)
- `--kafka-topic` : Topic Kafka (défaut: `weather_history`)
- `--bootstrap-servers` : Serveur Kafka (défaut: `localhost:29092`)
- `--hdfs-path` : Chemin HDFS de base (défaut: `/hdfs-data`)
- `--skip-kafka` : Ne pas envoyer à Kafka, seulement HDFS
- `--skip-hdfs` : Ne pas sauvegarder dans HDFS, seulement Kafka

## Structure HDFS

Les données sont sauvegardées dans HDFS avec la structure suivante :

```
/hdfs-data/
  ├── France/
  │   └── Paris/
  │       └── weather_history_raw/
  │           └── historical_data.json
  ├── USA/
  │   └── New_York/
  │       └── weather_history_raw/
  │           └── historical_data.json
  └── UK/
      └── London/
          └── weather_history_raw/
              └── historical_data.json
```

## Format des données

### Structure JSON sauvegardée

```json
{
  "metadata": {
    "location": {
      "latitude": 48.8566,
      "longitude": 2.3522,
      "city": "Paris",
      "country": "France",
      "location_name": "Paris, Île-de-France, France"
    },
    "period": {
      "start_date": "2014-01-12",
      "end_date": "2024-01-12",
      "years": 10
    },
    "downloaded_at": "2024-01-12T10:30:00.123456",
    "source": "open-meteo-archive"
  },
  "data": {
    "daily": {
      "time": ["2014-01-12", "2014-01-13", ...],
      "temperature_2m_max": [8.5, 9.2, ...],
      "temperature_2m_min": [2.1, 3.0, ...],
      "temperature_2m_mean": [5.3, 6.1, ...],
      "precipitation_sum": [0.0, 2.5, ...],
      "windspeed_10m_max": [12.3, 15.2, ...],
      "windspeed_10m_mean": [8.5, 10.1, ...],
      "winddirection_10m_dominant": [180, 190, ...]
    }
  }
}
```

### Données quotidiennes incluses

- **temperature_2m_max** : Température maximale quotidienne
- **temperature_2m_min** : Température minimale quotidienne
- **temperature_2m_mean** : Température moyenne quotidienne
- **precipitation_sum** : Précipitations totales quotidiennes
- **windspeed_10m_max** : Vitesse du vent maximale quotidienne
- **windspeed_10m_mean** : Vitesse du vent moyenne quotidienne
- **winddirection_10m_dominant** : Direction du vent dominante

## Test complet

### 1. Démarrer les services

```bash
docker-compose up -d
```

### 2. Créer le topic Kafka

```bash
./create_history_topic.sh
```

### 3. Télécharger les données historiques

```bash
python3 weather_history_loader.py --city Paris --country France --years 10
```

### 4. Vérifier les données dans HDFS

```bash
# Lister la structure
docker exec namenode hdfs dfs -ls -R /hdfs-data/France/Paris/

# Afficher un aperçu des données
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/weather_history_raw/historical_data.json | head -50
```

### 5. Vérifier les données dans Kafka

```bash
# Consommer depuis le topic
python3 kafka_consumer.py weather_history --from-beginning
```

## Vérification des données

### Dans HDFS

```bash
# Voir la taille du fichier
docker exec namenode hdfs dfs -du -h /hdfs-data/France/Paris/weather_history_raw/

# Compter le nombre de jours de données
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/weather_history_raw/historical_data.json | \
  python3 -c "import json, sys; data=json.load(sys.stdin); print(len(data['data']['daily']['time']))"
```

### Dans Kafka

```bash
# Vérifier les messages dans le topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather_history \
  --from-beginning \
  --max-messages 1
```

## Dépannage

### Erreur : "Aucune localisation trouvée"

- Vérifiez l'orthographe de la ville et du pays
- Essayez avec le nom en anglais si le nom français ne fonctionne pas
- Spécifiez le pays pour plus de précision

### Erreur : "Impossible de télécharger les données historiques"

- Vérifiez votre connexion Internet
- L'API archive peut avoir des limitations de taux
- Attendez quelques secondes et réessayez
- Vérifiez que les dates demandées sont valides

### Erreur : "HDFS n'est pas accessible"

Vérifiez que les conteneurs HDFS sont en cours d'exécution :

```bash
docker ps | grep -E "namenode|datanode"
```

### Les données ne sont pas complètes

- L'API archive peut ne pas avoir de données pour toutes les dates
- Certaines périodes peuvent avoir des valeurs `null`
- Vérifiez les métadonnées dans le fichier JSON pour voir la période réelle

### Timeout lors du téléchargement

- Pour de grandes périodes (10 ans), le téléchargement peut prendre du temps
- L'API peut limiter la taille des requêtes
- Essayez de diviser en plusieurs périodes plus courtes si nécessaire

## Exemples de villes

| Ville | Pays | Commande |
|-------|------|----------|
| Paris | France | `--city Paris --country France` |
| Londres | UK | `--city London --country UK` |
| New York | USA | `--city "New York" --country USA` |
| Tokyo | Japon | `--city Tokyo --country Japan` |
| Sydney | Australie | `--city Sydney --country Australia` |

## Utilisation des données historiques

Les données historiques peuvent être utilisées pour :

1. **Analyse de tendances** : Évolution du climat sur plusieurs années
2. **Comparaisons** : Comparer les conditions actuelles avec les moyennes historiques
3. **Prédictions** : Entraîner des modèles de prédiction météo
4. **Visualisations** : Créer des graphiques d'évolution long terme
5. **Alertes** : Déterminer si les conditions actuelles sont exceptionnelles

## Notes importantes

- **Temps de téléchargement** : 10 ans de données peuvent prendre 30 secondes à plusieurs minutes
- **Taille des données** : Un fichier de 10 ans peut faire plusieurs MB
- **Format** : Les données sont sauvegardées en JSON brut dans HDFS
- **API gratuite** : Pas de clé API requise, mais respectez les limites d'utilisation
- **Disponibilité** : Les données historiques peuvent ne pas être disponibles pour toutes les localisations

## Exemple de workflow complet

```bash
# 1. Créer le topic
./create_history_topic.sh

# 2. Télécharger les données
python3 weather_history_loader.py --city Paris --country France --years 10

# 3. Vérifier dans HDFS
docker exec namenode hdfs dfs -ls -R /hdfs-data/France/Paris/

# 4. Analyser les données (optionnel)
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/weather_history_raw/historical_data.json | \
  python3 -c "import json, sys; data=json.load(sys.stdin); print(f\"Jours: {len(data['data']['daily']['time'])}\")"
```
