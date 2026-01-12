# Exercice 4 : Transformation des données et détection d'alertes

## Description

Cet exercice consiste à traiter le flux `weather_stream` en temps réel avec Spark Streaming pour transformer les données et détecter des alertes météo, puis produire un nouveau topic `weather_transformed` avec les données enrichies.

## Prérequis

1. **Services Docker démarrés** (Kafka, Spark, pyspark-notebook)
2. **Topic `weather_stream`** avec des données (voir Exercice 3)
3. **Topic `weather_transformed`** créé (script fourni)

## Installation et configuration

### 1. Démarrer les services

```bash
docker-compose up -d
```

### 2. Créer le topic weather_transformed

```bash
./create_transformed_topic.sh
```

Ou manuellement :

```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic weather_transformed \
  --partitions 1 \
  --replication-factor 1
```

## Utilisation

### Option 1 : Script automatique (recommandé)

```bash
./run_weather_transformer.sh
```

### Option 2 : Exécution manuelle

```bash
# 1. Copier le script dans le conteneur
docker cp weather_transformer.py pyspark_notebook:/home/jovyan/work/

# 2. Exécuter le transformateur avec spark-submit
docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master local[*] \
  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint/weather_transformer \
  /home/jovyan/work/weather_transformer.py \
  --kafka-servers kafka:9092 \
  --input-topic weather_stream \
  --output-topic weather_transformed
```

**Note** : Le package Kafka (`spark-sql-kafka-0-10_2.12`) sera téléchargé automatiquement au premier lancement. Cela peut prendre quelques minutes.

## Règles de transformation

### Alertes de vent

- **Vent faible** (< 10 m/s) → `level_0`
- **Vent modéré** (10–20 m/s) → `level_1`
- **Vent fort** (> 20 m/s) → `level_2`

### Alertes de chaleur

- **Température normale** (< 25°C) → `level_0`
- **Chaleur modérée** (25–35°C) → `level_1`
- **Canicule** (> 35°C) → `level_2`

## Structure des données transformées

Le topic `weather_transformed` contient des messages JSON avec la structure suivante :

```json
{
  "event_time": "2024-01-12T10:30:00.123456",
  "latitude": 48.8566,
  "longitude": 2.3522,
  "temperature": 15.5,
  "windspeed": 10.2,
  "winddirection": 180,
  "weathercode": 61,
  "weather_time": "2024-01-12T10:00",
  "wind_alert_level": "level_1",
  "heat_alert_level": "level_0",
  "original_timestamp": "2024-01-12T10:30:00.123456",
  "data_source": "open-meteo"
}
```

### Colonnes ajoutées

- **`event_time`** : Timestamp de traitement (généré par Spark)
- **`temperature`** : Température en °C (extraite et transformée)
- **`windspeed`** : Vitesse du vent en m/s (extraite et transformée)
- **`wind_alert_level`** : Niveau d'alerte vent (`level_0`, `level_1`, `level_2`)
- **`heat_alert_level`** : Niveau d'alerte chaleur (`level_0`, `level_1`, `level_2`)

## Test complet

### 1. Démarrer les services

```bash
docker-compose up -d
```

### 2. Créer les topics

```bash
# Créer weather_stream (si pas déjà fait)
./setup_exercise1.sh

# Créer weather_transformed
./create_transformed_topic.sh
```

### 3. Dans un premier terminal, lancer le producteur météo

```bash
python3 current_weather.py 48.8566 2.3522 --continuous --interval 30
```

### 4. Dans un second terminal, lancer le transformateur Spark

```bash
./run_weather_transformer.sh
```

### 5. Dans un troisième terminal, vérifier les données transformées

```bash
python3 kafka_consumer.py weather_transformed --from-beginning
```

Vous devriez voir les données transformées avec les niveaux d'alerte calculés !

## Architecture du flux de données

```
┌─────────────────┐
│  Open-Meteo API │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ current_weather │ (Producteur)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ weather_stream  │ (Topic Kafka)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ weather_        │ (Spark Streaming)
│ transformer     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ weather_         │ (Topic Kafka)
│ transformed     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ kafka_consumer   │ (Consommateur)
└─────────────────┘
```

## Options du transformateur

- `--kafka-servers` : Serveurs Kafka bootstrap (défaut: `kafka:9092`)
- `--input-topic` : Topic source (défaut: `weather_stream`)
- `--output-topic` : Topic destination (défaut: `weather_transformed`)

## Dépannage

### Erreur : "Topic weather_transformed does not exist"

Créez le topic :
```bash
./create_transformed_topic.sh
```

### Erreur : "No messages in weather_stream"

Assurez-vous que le producteur météo est en cours d'exécution :
```bash
python3 current_weather.py 48.8566 2.3522 --continuous
```

### Erreur de connexion Spark à Kafka

Vérifiez que :
- Kafka est accessible depuis le conteneur Spark
- Le nom d'hôte `kafka` est résolu (dans Docker Compose)
- Les ports sont correctement configurés

### Le transformateur ne traite pas les messages

- Vérifiez les logs du conteneur Spark
- Assurez-vous que `weather_stream` contient des messages
- Vérifiez que le checkpoint n'est pas corrompu (supprimez `/tmp/checkpoint/weather_transformer` si nécessaire)

### Erreur : "Multiple streaming queries are concurrently using checkpoint"

Cette erreur se produit si plusieurs instances du transformateur tentent d'utiliser le même checkpoint. Solutions :

1. **Arrêter toutes les instances en cours** :
   ```bash
   docker exec pyspark_notebook pkill -f weather_transformer
   ```

2. **Nettoyer le checkpoint** :
   ```bash
   ./clean_checkpoint.sh
   ```

3. **Relancer le transformateur** :
   ```bash
   ./run_weather_transformer.sh
   ```

Le script `run_weather_transformer.sh` nettoie automatiquement les checkpoints avant de démarrer.

### Erreur : "ModuleNotFoundError: No module named 'pyspark'"

Le script doit être exécuté avec `spark-submit`, pas directement avec Python. Utilisez le script `run_weather_transformer.sh` qui gère cela automatiquement.

### Erreur lors du téléchargement du package Kafka

Si le package Kafka ne se télécharge pas correctement, vous pouvez :
1. Vérifier votre connexion Internet
2. Essayer une version différente du package (3.3.0 ou 3.4.0 au lieu de 3.5.0)
3. Télécharger le package manuellement et le placer dans le classpath Spark

## Notes importantes

- **Checkpoint** : Spark utilise un checkpoint pour la récupération en cas d'erreur. Il est stocké dans `/tmp/checkpoint/weather_transformer` dans le conteneur.
- **Mode streaming** : Le transformateur traite les messages en temps réel au fur et à mesure qu'ils arrivent.
- **Performance** : Pour de gros volumes, augmentez le nombre de partitions du topic.
- **Arrêt propre** : Utilisez `Ctrl+C` pour arrêter le transformateur proprement.

## Exemples de données transformées

### Exemple 1 : Vent modéré, température normale

```json
{
  "temperature": 20.5,
  "windspeed": 15.2,
  "wind_alert_level": "level_1",
  "heat_alert_level": "level_0"
}
```

### Exemple 2 : Vent fort, canicule

```json
{
  "temperature": 38.0,
  "windspeed": 25.5,
  "wind_alert_level": "level_2",
  "heat_alert_level": "level_2"
}
```

### Exemple 3 : Vent faible, chaleur modérée

```json
{
  "temperature": 28.0,
  "windspeed": 5.5,
  "wind_alert_level": "level_0",
  "heat_alert_level": "level_1"
}
```
