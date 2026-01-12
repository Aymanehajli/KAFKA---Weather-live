# Exercice 10 : Détection des records climatiques locaux

## Description

Cet exercice consiste à créer un job Spark qui analyse les données historiques stockées dans HDFS, calcule les records climatiques pour chaque ville, et émet ces records dans Kafka et HDFS pour exploitation par un dashboard.

## Prérequis

1. **Services Docker démarrés** (Kafka, HDFS, Spark)
2. **Données historiques dans HDFS** (voir Exercice 9)
3. **Topic Kafka `weather_records`** (créé automatiquement par le script)

## Records calculés

Pour chaque ville, le job calcule :

### 1. Jour le plus chaud de la décennie
- **Date** du jour avec la température maximale la plus élevée
- **Température** maximale enregistrée

### 2. Jour le plus froid de la décennie
- **Date** du jour avec la température minimale la plus basse
- **Température** minimale enregistrée

### 3. Rafale de vent la plus forte
- **Date** du jour avec la vitesse de vent maximale la plus élevée
- **Vitesse du vent** maximale enregistrée (m/s)

### 4. Jour le plus pluvieux
- **Date** du jour avec les précipitations les plus importantes
- **Précipitations** totales enregistrées (mm)

## Utilisation

### Option 1 : Script automatique (recommandé)

```bash
./run_records_analyzer.sh
```

### Option 2 : Exécution manuelle

```bash
# 1. Copier le script dans le conteneur
docker cp weather_records_analyzer.py pyspark_notebook:/home/jovyan/work/

# 2. Exécuter avec spark-submit
docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --master local[*] \
  /home/jovyan/work/weather_records_analyzer.py \
  --hdfs-path /hdfs-data \
  --kafka-servers kafka:9092 \
  --kafka-topic weather_records
```

## Options disponibles

- `--hdfs-path` : Chemin HDFS de base (défaut: `/hdfs-data`)
- `--kafka-servers` : Serveurs Kafka bootstrap (défaut: `localhost:29092`)
- `--kafka-topic` : Topic Kafka (défaut: `weather_records`)
- `--skip-kafka` : Ne pas envoyer à Kafka, seulement HDFS
- `--skip-hdfs` : Ne pas sauvegarder dans HDFS, seulement Kafka

## Structure HDFS des records

Les records sont sauvegardés dans HDFS avec la structure suivante :

```
/hdfs-data/
  ├── France/
  │   └── Paris/
  │       └── weather_records/
  │           └── records.json
  ├── USA/
  │   └── New_York/
  │       └── weather_records/
  │           └── records.json
  └── UK/
      └── London/
          └── weather_records/
              └── records.json
```

## Format des records

### Structure JSON sauvegardée

```json
{
  "city": "Paris",
  "country": "France",
  "latitude": 48.8566,
  "longitude": 2.3522,
  "records": {
    "hottest_day": {
      "date": "2019-07-25",
      "temperature": 42.6
    },
    "coldest_day": {
      "date": "2018-02-28",
      "temperature": -10.2
    },
    "strongest_wind": {
      "date": "2020-12-10",
      "windspeed": 28.5
    },
    "rainiest_day": {
      "date": "2016-06-01",
      "precipitation": 45.8
    }
  },
  "statistics": {
    "total_days": 3650
  }
}
```

### Format Kafka

Les records envoyés à Kafka ont le même format, avec en plus :
- `computed_at` : Timestamp de calcul du record

## Test complet

### 1. Préparer les données historiques

```bash
# Télécharger 10 ans de données pour Paris
python3 weather_history_loader.py --city Paris --country France --years 10

# Télécharger pour d'autres villes si nécessaire
python3 weather_history_loader.py --city "New York" --country USA --years 10
```

### 2. Exécuter l'analyseur

```bash
./run_records_analyzer.sh
```

### 3. Vérifier les records dans HDFS

```bash
# Lister la structure
docker exec namenode hdfs dfs -ls -R /hdfs-data/France/Paris/

# Afficher les records
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/weather_records/records.json
```

### 4. Vérifier les records dans Kafka

```bash
# Consommer depuis le topic
python3 kafka_consumer.py weather_records --from-beginning
```

## Exemple de sortie

```
==========================================
📊 Analyseur de records climatiques
==========================================

📂 Lecture des données historiques depuis HDFS: /hdfs-data
📄 1 fichier(s) historique(s) trouvé(s)
✅ 3650 jour(s) de données chargé(s)

📊 Calcul des records climatiques...

✅ 1 ville(s) analysée(s)

📋 Aperçu des records:
+-------+-----+--------+---------+-------------+---------------+-------------+---------------+----------------+------------------+
|country|city |latitude|longitude|max_temp_date|max_temperature|min_temp_date|min_temperature|max_wind_date   |max_windspeed     |
+-------+-----+--------+---------+-------------+---------------+-------------+---------------+----------------+------------------+
|France |Paris|48.8566 |2.3522   |2019-07-25   |42.6           |2018-02-28   |-10.2          |2020-12-10      |28.5              |
+-------+-----+--------+---------+-------------+---------------+-------------+---------------+----------------+------------------+

💾 Sauvegarde des records dans HDFS...
✅ Records sauvegardés: /hdfs-data/France/Paris/weather_records/records.json

📤 Envoi des records à Kafka: weather_records...
✅ Record envoyé: Paris, France
✅ 1 record(s) envoyé(s) à Kafka

==========================================
✅ Analyse terminée!
==========================================
```

## Vérification des records

### Dans HDFS

```bash
# Afficher les records pour Paris
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/weather_records/records.json | python3 -m json.tool
```

### Dans Kafka

```bash
# Consommer les records
python3 kafka_consumer.py weather_records --from-beginning
```

### Interface Web HDFS

Ouvrir `http://localhost:9870` et naviguer vers `/hdfs-data/{country}/{city}/weather_records/records.json`

## Dépannage

### Erreur : "Aucune donnée historique disponible"

Assurez-vous d'avoir exécuté l'exercice 9 pour télécharger les données historiques :

```bash
python3 weather_history_loader.py --city Paris --country France --years 10
```

### Erreur : "Aucun fichier historique trouvé dans HDFS"

Vérifiez que les fichiers existent :

```bash
docker exec namenode hdfs dfs -ls -R /hdfs-data | grep weather_history_raw
```

### Erreur lors de l'envoi à Kafka

- Vérifiez que Kafka est accessible
- Vérifiez que le topic `weather_records` existe
- Consultez les logs pour plus de détails

### Les dates des records ne sont pas correctes

- Vérifiez que les données historiques contiennent des dates valides
- Vérifiez que les données ne contiennent pas trop de valeurs nulles
- Le script utilise la première occurrence en cas d'égalité (ordre par date décroissante)

## Utilisation des records pour un dashboard

Les records peuvent être consommés depuis Kafka pour alimenter un dashboard :

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'weather_records',
    bootstrap_servers='localhost:29092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    record = message.value
    city = record['city']
    country = record['country']
    hottest = record['records']['hottest_day']
    coldest = record['records']['coldest_day']
    
    print(f"{city}, {country}:")
    print(f"  Plus chaud: {hottest['temperature']}°C le {hottest['date']}")
    print(f"  Plus froid: {coldest['temperature']}°C le {coldest['date']}")
```

## Notes importantes

- **Performance** : Pour de grandes quantités de données, le traitement peut prendre plusieurs minutes
- **Données manquantes** : Les jours avec des valeurs nulles sont exclus du calcul
- **Égalités** : En cas d'égalité, la date la plus récente est choisie
- **Format** : Les records sont sauvegardés en JSON dans HDFS et envoyés en JSON à Kafka

## Exemple de workflow complet

```bash
# 1. Télécharger les données historiques
python3 weather_history_loader.py --city Paris --country France --years 10

# 2. Analyser les records
./run_records_analyzer.sh

# 3. Visualiser les résultats
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/weather_records/records.json | python3 -m json.tool
```
