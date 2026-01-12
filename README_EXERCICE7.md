# Exercice 7 : Stockage dans HDFS organisé

## Description

Cet exercice consiste à créer un consommateur Kafka qui lit depuis le topic `weather_transformed`, filtre les messages contenant des alertes (level_1 ou level_2), et les sauvegarde dans HDFS avec une structure organisée par pays et ville.

## Prérequis

1. **Services Docker démarrés** (Kafka, HDFS namenode/datanode)
2. **Topic `weather_transformed`** avec des données (voir Exercice 4)
3. **HDFS accessible** depuis le conteneur namenode

## Structure HDFS

Les alertes sont sauvegardées dans HDFS avec la structure suivante :

```
/hdfs-data/
  ├── France/
  │   ├── Paris/
  │   │   └── alerts.json
  │   └── Lyon/
  │       └── alerts.json
  ├── USA/
  │   └── New_York/
  │       └── alerts.json
  └── UK/
      └── London/
          └── alerts.json
```

### Format des fichiers

Chaque fichier `alerts.json` contient les alertes au format JSON Lines (une alerte par ligne) :

```json
{"timestamp": "2024-01-12T10:30:00.123456", "alert_data": {...}}
{"timestamp": "2024-01-12T10:35:00.123456", "alert_data": {...}}
```

## Utilisation

### Démarrage simple

```bash
python3 hdfs_alert_saver.py
```

### Options disponibles

- `--topic` : Nom du topic Kafka (défaut: `weather_transformed`)
- `--bootstrap-servers` : Adresse du serveur Kafka (défaut: `localhost:29092`)
- `--group-id` : ID du groupe de consommateurs (défaut: `hdfs-alert-saver`)
- `--hdfs-path` : Chemin de base HDFS (défaut: `/hdfs-data`)

### Exemples

#### Utilisation par défaut

```bash
python3 hdfs_alert_saver.py
```

#### Spécifier un chemin HDFS personnalisé

```bash
python3 hdfs_alert_saver.py --hdfs-path /data/weather-alerts
```

#### Spécifier un topic différent

```bash
python3 hdfs_alert_saver.py --topic my_weather_topic
```

## Critères de filtrage des alertes

Un message est considéré comme une alerte si :

- **`wind_alert_level`** est `level_1` ou `level_2` (vent modéré ou fort)
- **OU** **`heat_alert_level`** est `level_1` ou `level_2` (chaleur modérée ou canicule)

### Exemples d'alertes

#### Alerte de vent

```json
{
  "wind_alert_level": "level_2",
  "heat_alert_level": "level_0",
  "windspeed": 25.5,
  "temperature": 18.0,
  "city": "Paris",
  "country": "France"
}
```

#### Alerte de chaleur

```json
{
  "wind_alert_level": "level_0",
  "heat_alert_level": "level_2",
  "windspeed": 5.0,
  "temperature": 38.5,
  "city": "Paris",
  "country": "France"
}
```

## Test complet

### 1. Démarrer les services

```bash
docker-compose up -d
```

### 2. Vérifier que HDFS est accessible

```bash
docker exec namenode hdfs dfs -ls /
```

### 3. Dans un premier terminal, lancer le producteur météo

```bash
python3 current_weather.py --city Paris --country France --continuous --interval 30
```

### 4. Dans un second terminal, lancer le transformateur (si pas déjà en cours)

```bash
./run_weather_transformer.sh
```

### 5. Dans un troisième terminal, lancer le consommateur HDFS

```bash
python3 hdfs_alert_saver.py
```

Le consommateur affichera les alertes détectées et les sauvegardera dans HDFS.

### 6. Vérifier les fichiers dans HDFS

```bash
# Lister la structure
docker exec namenode hdfs dfs -ls -R /hdfs-data

# Afficher le contenu d'un fichier d'alertes
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/alerts.json
```

## Vérification des données dans HDFS

### Lister la structure des répertoires

```bash
docker exec namenode hdfs dfs -ls -R /hdfs-data
```

### Afficher le contenu d'un fichier d'alertes

```bash
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/alerts.json
```

### Compter le nombre d'alertes par ville

```bash
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/alerts.json | wc -l
```

### Rechercher des alertes spécifiques

```bash
# Alertes de vent fort (level_2)
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/alerts.json | \
  grep -i '"wind_alert_level": "level_2"'

# Alertes de canicule (level_2)
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/alerts.json | \
  grep -i '"heat_alert_level": "level_2"'
```

## Interface Web HDFS

Vous pouvez également visualiser les fichiers via l'interface web HDFS :

1. Ouvrir un navigateur : `http://localhost:9870`
2. Naviguer vers : **Utilities** → **Browse the file system**
3. Aller dans `/hdfs-data` pour voir la structure organisée

## Dépannage

### Erreur : "HDFS n'est pas accessible"

Vérifiez que les conteneurs HDFS sont en cours d'exécution :

```bash
docker ps | grep -E "namenode|datanode"
```

Si les conteneurs ne sont pas démarrés :

```bash
docker-compose up -d namenode datanode
```

Attendez quelques secondes que HDFS soit prêt, puis réessayez.

### Erreur : "Permission denied" dans HDFS

Les fichiers sont créés avec les permissions par défaut. Si nécessaire, ajustez les permissions :

```bash
docker exec namenode hdfs dfs -chmod -R 755 /hdfs-data
```

### Aucune alerte sauvegardée

- Vérifiez que le producteur météo envoie des données
- Vérifiez que le transformateur traite les données
- Vérifiez que les conditions météo génèrent des alertes (vent > 10 m/s ou température > 25°C)
- Consultez les logs du consommateur pour voir les messages traités

### Les fichiers ne sont pas créés dans la bonne structure

- Vérifiez que les messages contiennent les champs `city` et `country`
- Si ces champs sont absents, le consommateur utilisera les coordonnées comme fallback
- Assurez-vous d'utiliser le producteur avec `--city` et `--country` (voir Exercice 6)

## Format des données sauvegardées

Chaque ligne du fichier `alerts.json` contient :

```json
{
  "timestamp": "2024-01-12T10:30:00.123456",
  "alert_data": {
    "event_time": "2024-01-12T10:30:00.123456",
    "latitude": 48.8566,
    "longitude": 2.3522,
    "city": "Paris",
    "country": "France",
    "location_name": "Paris, Île-de-France, France",
    "temperature": 38.5,
    "windspeed": 25.2,
    "wind_alert_level": "level_2",
    "heat_alert_level": "level_2",
    ...
  }
}
```

## Avantages de cette structure

1. **Organisation par localisation** : Facilite la recherche d'alertes par ville ou pays
2. **Partitionnement HDFS** : Optimise les requêtes et le stockage
3. **Scalabilité** : Chaque ville a son propre fichier, évitant les fichiers trop volumineux
4. **Historique** : Toutes les alertes sont conservées avec leur timestamp

## Notes importantes

- **Format JSON Lines** : Chaque alerte est sur une ligne séparée (format JSONL)
- **Append** : Les nouvelles alertes sont ajoutées au fichier existant
- **Nettoyage** : Les fichiers temporaires sont automatiquement supprimés
- **Fallback** : Si ville/pays ne sont pas disponibles, utilise les coordonnées

## Exemple de workflow complet

```bash
# Terminal 1: Producteur
python3 current_weather.py --city Paris --country France --continuous

# Terminal 2: Transformateur
./run_weather_transformer.sh

# Terminal 3: Sauvegarde HDFS
python3 hdfs_alert_saver.py

# Terminal 4: Vérification
docker exec namenode hdfs dfs -ls -R /hdfs-data
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/alerts.json | head -5
```
