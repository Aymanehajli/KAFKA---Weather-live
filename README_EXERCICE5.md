# Exercice 5 : Agrégats en temps réel avec Spark

## Description

Cet exercice consiste à calculer des agrégats en temps réel sur le flux `weather_transformed` en utilisant des fenêtres glissantes (sliding windows) avec Spark Streaming.

## Prérequis

1. **Services Docker démarrés** (Kafka, Spark, pyspark-notebook)
2. **Topic `weather_transformed`** avec des données (voir Exercice 4)
3. **Flux de données actif** depuis le producteur météo

## Fonctionnalités implémentées

### Fenêtres glissantes

- **Fenêtre de 5 minutes** avec **glissement de 1 minute** (par défaut)
- Configurable via les arguments `--window` et `--slide`
- Support des fenêtres de 1 minute avec glissement de 30 secondes

### Métriques calculées

#### 1. Alertes par type

- **Nombre d'alertes de vent** (level_1 et level_2)
- **Nombre d'alertes de chaleur** (level_1 et level_2)
- **Détail par niveau** :
  - `wind_level_1_count` : Alertes de vent niveau 1
  - `wind_level_2_count` : Alertes de vent niveau 2
  - `heat_level_1_count` : Alertes de chaleur niveau 1
  - `heat_level_2_count` : Alertes de chaleur niveau 2
- **Nombre total d'alertes** (tous types confondus)

#### 2. Statistiques de température

- **Moyenne** (`avg_temperature`)
- **Minimum** (`min_temperature`)
- **Maximum** (`max_temperature`)

#### 3. Agrégats par localisation

- **Agrégats par coordonnées** (latitude, longitude)
- **Nombre total de messages** par localisation
- **Période de la fenêtre** (début et fin)

## Utilisation

### Option 1 : Script automatique (recommandé)

```bash
./run_weather_aggregator.sh
```

### Option 2 : Exécution manuelle

```bash
# 1. Copier le script dans le conteneur
docker cp weather_aggregator.py pyspark_notebook:/home/jovyan/work/

# 2. Exécuter l'agrégateur avec spark-submit
docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master local[*] \
  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint/weather_aggregator \
  /home/jovyan/work/weather_aggregator.py \
  --kafka-servers kafka:9092 \
  --input-topic weather_transformed \
  --window "5 minutes" \
  --slide "1 minute"
```

### Options disponibles

- `--kafka-servers` : Serveurs Kafka bootstrap (défaut: `kafka:9092`)
- `--input-topic` : Topic source (défaut: `weather_transformed`)
- `--window` : Durée de la fenêtre (défaut: `5 minutes`)
- `--slide` : Durée du glissement (défaut: `1 minute`)

### Exemples de configurations

#### Fenêtre de 5 minutes, glissement de 1 minute (par défaut)

```bash
./run_weather_aggregator.sh
```

#### Fenêtre de 1 minute, glissement de 30 secondes

```bash
docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master local[*] \
  /home/jovyan/work/weather_aggregator.py \
  --window "1 minute" \
  --slide "30 seconds"
```

#### Fenêtre de 10 minutes, glissement de 2 minutes

```bash
docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master local[*] \
  /home/jovyan/work/weather_aggregator.py \
  --window "10 minutes" \
  --slide "2 minutes"
```

## Structure des données agrégées

Les agrégats calculés contiennent les champs suivants :

```json
{
  "window_start": "2024-01-12T10:00:00.000Z",
  "window_end": "2024-01-12T10:05:00.000Z",
  "location": "48.8566,2.3522",
  "latitude": 48.8566,
  "longitude": 2.3522,
  "avg_temperature": 18.5,
  "min_temperature": 15.2,
  "max_temperature": 22.1,
  "wind_alerts_count": 3,
  "heat_alerts_count": 1,
  "wind_level_1_count": 2,
  "wind_level_2_count": 1,
  "heat_level_1_count": 1,
  "heat_level_2_count": 0,
  "total_alerts_count": 4,
  "total_messages": 10,
  "computed_at": "2024-01-12T10:01:00.000Z"
}
```

## Test complet

### 1. Démarrer tous les services

```bash
docker-compose up -d
```

### 2. Créer les topics nécessaires

```bash
# Créer weather_stream (si pas déjà fait)
./setup_exercise1.sh

# Créer weather_transformed (si pas déjà fait)
./create_transformed_topic.sh
```

### 3. Dans un premier terminal, lancer le producteur météo

```bash
python3 current_weather.py 48.8566 2.3522 --continuous --interval 30
```

### 4. Dans un second terminal, lancer le transformateur (si pas déjà en cours)

```bash
./run_weather_transformer.sh
```

### 5. Dans un troisième terminal, lancer l'agrégateur

```bash
./run_weather_aggregator.sh
```

L'agrégateur affichera les métriques calculées toutes les minutes (selon le glissement configuré).

## Architecture du flux de données

```
Open-Meteo API
    ↓
current_weather.py
    ↓
weather_stream
    ↓
weather_transformer.py
    ↓
weather_transformed
    ↓
weather_aggregator.py
    ↓
Agrégats affichés en temps réel
```

## Exemples de sortie

### Résumé des alertes

```
+------------------+------------------+------------------+------------------+
|location          |wind_alerts_count|heat_alerts_count|total_alerts_count|
+------------------+------------------+------------------+------------------+
|48.8566,2.3522    |3                |1                |4                |
+------------------+------------------+------------------+------------------+
```

### Résumé des températures

```
+------------------+----------------+----------------+----------------+
|location          |avg_temperature |min_temperature |max_temperature |
+------------------+----------------+----------------+----------------+
|48.8566,2.3522    |18.5            |15.2            |22.1            |
+------------------+----------------+----------------+----------------+
```

## Comprendre les fenêtres glissantes

### Fenêtre de 5 minutes, glissement de 1 minute

- **10:00 - 10:05** : Première fenêtre
- **10:01 - 10:06** : Deuxième fenêtre (glissée de 1 minute)
- **10:02 - 10:07** : Troisième fenêtre (glissée de 1 minute)
- etc.

Chaque fenêtre contient les données des 5 dernières minutes, mais les calculs sont effectués toutes les minutes.

### Fenêtre de 1 minute, glissement de 30 secondes

- **10:00:00 - 10:01:00** : Première fenêtre
- **10:00:30 - 10:01:30** : Deuxième fenêtre (glissée de 30 secondes)
- **10:01:00 - 10:02:00** : Troisième fenêtre (glissée de 30 secondes)
- etc.

## Dépannage

### Erreur : "Topic weather_transformed does not exist"

Assurez-vous d'avoir exécuté l'exercice 4 pour créer le topic `weather_transformed`.

### Aucun agrégat calculé

- Vérifiez que le transformateur est en cours d'exécution
- Vérifiez que le producteur météo envoie des données
- Attendez quelques minutes pour que les fenêtres se remplissent

### Erreur : "Multiple streaming queries are concurrently using checkpoint"

Nettoyez les checkpoints :
```bash
docker exec pyspark_notebook bash -c "rm -rf /tmp/checkpoint/weather_aggregator*"
```

### Les agrégats ne s'affichent pas

- Vérifiez que vous avez assez de données dans la fenêtre
- Réduisez la taille de la fenêtre (ex: `--window "1 minute"`)
- Vérifiez les logs du conteneur Spark

## Notes importantes

- **Watermark** : Un watermark de 10 minutes est configuré pour gérer les données en retard
- **Mode de sortie** : `update` - affiche uniquement les fenêtres mises à jour
- **Checkpoint** : Les checkpoints sont stockés dans `/tmp/checkpoint/weather_aggregator`
- **Performance** : Pour de gros volumes, augmentez les ressources Spark

## Métriques détaillées

### Alertes de vent

- **wind_alerts_count** : Nombre total d'alertes de vent (level_1 + level_2)
- **wind_level_1_count** : Alertes de vent modéré (10-20 m/s)
- **wind_level_2_count** : Alertes de vent fort (> 20 m/s)

### Alertes de chaleur

- **heat_alerts_count** : Nombre total d'alertes de chaleur (level_1 + level_2)
- **heat_level_1_count** : Chaleur modérée (25-35°C)
- **heat_level_2_count** : Canicule (> 35°C)

### Température

- **avg_temperature** : Température moyenne sur la fenêtre
- **min_temperature** : Température minimale sur la fenêtre
- **max_temperature** : Température maximale sur la fenêtre

### Localisation

- **location** : Clé de localisation (format: "latitude,longitude")
- **latitude** : Latitude moyenne
- **longitude** : Longitude moyenne
- **total_messages** : Nombre total de messages dans la fenêtre
