# Exercice 6 : Extension du producteur avec géocodage

## Description

Cet exercice étend le producteur météo pour accepter ville et pays comme arguments, utilise l'API de géocodage Open-Meteo pour obtenir les coordonnées, et enrichit les messages avec ces informations pour permettre le partitionnement HDFS et les agrégats par région.

## Prérequis

1. **Services Docker démarrés** (Kafka, Spark)
2. **Producteur météo** (version étendue)
3. **API Open-Meteo** accessible (géocodage gratuit)

## Fonctionnalités ajoutées

### 1. Géocodage automatique

- Utilisation de l'API de géocodage Open-Meteo : `https://geocoding-api.open-meteo.com/v1/search`
- Conversion automatique ville/pays → coordonnées (latitude, longitude)
- Support des noms de villes en français et anglais

### 2. Enrichissement des messages

Chaque message produit inclut maintenant :
- **`city`** : Nom de la ville
- **`country`** : Nom du pays
- **`location_name`** : Nom complet de la localisation (ville, région, pays)

### 3. Partitionnement HDFS

- Clé de partitionnement basée sur `country/city` (format: `France/Paris`)
- Permet un partitionnement efficace pour le stockage HDFS
- Facilite les agrégats par région

## Utilisation

### Mode ville/pays (géocodage automatique)

#### Exemples de base

```bash
# Paris, France
python3 current_weather.py --city Paris --country France

# New York, USA
python3 current_weather.py --city "New York" --country USA

# London, UK
python3 current_weather.py --city London --country UK
```

#### Mode continu avec ville/pays

```bash
# Paris en mode continu (toutes les 60 secondes)
python3 current_weather.py --city Paris --country France --continuous

# New York avec intervalle personnalisé (30 secondes)
python3 current_weather.py --city "New York" --country USA --continuous --interval 30
```

### Mode coordonnées (compatible avec l'exercice 3)

```bash
# Avec coordonnées directes
python3 current_weather.py --lat 48.8566 --lon 2.3522

# Mode continu avec coordonnées
python3 current_weather.py --lat 48.8566 --lon 2.3522 --continuous
```

## Options disponibles

### Arguments de localisation

- `--city` ou `--city` : Nom de la ville (avec géocodage)
- `--country` : Nom du pays (optionnel mais recommandé pour plus de précision)
- `--lat` ou `--latitude` : Latitude (mode coordonnées)
- `--lon` ou `--longitude` : Longitude (mode coordonnées)

### Arguments généraux

- `--bootstrap-servers` : Adresse du serveur Kafka (défaut: `localhost:29092`)
- `--topic` : Nom du topic Kafka (défaut: `weather_stream`)
- `--continuous` : Mode continu (interroge l'API périodiquement)
- `--interval` : Intervalle en secondes pour le mode continu (défaut: 60)

## Structure des données enrichies

### Format des messages (weather_stream)

```json
{
  "timestamp": "2024-01-12T10:30:00.123456",
  "location": {
    "latitude": 48.8566,
    "longitude": 2.3522,
    "city": "Paris",
    "country": "France",
    "location_name": "Paris, Île-de-France, France"
  },
  "current_weather": {
    "temperature": 15.5,
    "windspeed": 10.2,
    "winddirection": 180,
    "weathercode": 61,
    "time": "2024-01-12T10:00"
  },
  "source": "open-meteo"
}
```

### Format des messages transformés (weather_transformed)

```json
{
  "event_time": "2024-01-12T10:30:00.123456",
  "latitude": 48.8566,
  "longitude": 2.3522,
  "city": "Paris",
  "country": "France",
  "location_name": "Paris, Île-de-France, France",
  "temperature": 15.5,
  "windspeed": 10.2,
  "wind_alert_level": "level_1",
  "heat_alert_level": "level_0",
  ...
}
```

## Partitionnement HDFS

### Clé de partitionnement

Les messages utilisent une clé basée sur `country/city` pour le partitionnement :

- **Format** : `{country}/{city}`
- **Exemples** :
  - `France/Paris`
  - `USA/New York`
  - `UK/London`

### Avantages

1. **Stockage HDFS** : Permet un partitionnement efficace par pays et ville
2. **Agrégats par région** : Facilite les calculs d'agrégats par ville ou pays
3. **Performance** : Améliore les requêtes filtrées par localisation

## Agrégats par région

Avec les informations de ville et pays, vous pouvez maintenant :

### 1. Agréger par ville

```sql
SELECT city, COUNT(*) as total_alerts
FROM weather_transformed
WHERE wind_alert_level IN ('level_1', 'level_2')
GROUP BY city
```

### 2. Agréger par pays

```sql
SELECT country, COUNT(*) as total_alerts
FROM weather_transformed
WHERE heat_alert_level IN ('level_1', 'level_2')
GROUP BY country
```

### 3. Agréger par région (ville + pays)

```sql
SELECT country, city, AVG(temperature) as avg_temp
FROM weather_transformed
GROUP BY country, city
```

## Test complet

### 1. Démarrer les services

```bash
docker-compose up -d
```

### 2. Créer les topics

```bash
./setup_exercise1.sh
./create_transformed_topic.sh
```

### 3. Dans un premier terminal, lancer le producteur avec ville/pays

```bash
python3 current_weather.py --city Paris --country France --continuous --interval 30
```

### 4. Dans un second terminal, lancer le transformateur

```bash
./run_weather_transformer.sh
```

### 5. Dans un troisième terminal, lancer l'agrégateur

```bash
./run_weather_aggregator.sh
```

### 6. Dans un quatrième terminal, vérifier les données

```bash
python3 kafka_consumer.py weather_transformed --from-beginning
```

Vous devriez voir les messages enrichis avec les informations de ville et pays !

## Exemples de villes

| Ville | Pays | Commande |
|-------|------|----------|
| Paris | France | `--city Paris --country France` |
| Londres | UK | `--city London --country UK` |
| New York | USA | `--city "New York" --country USA` |
| Tokyo | Japon | `--city Tokyo --country Japan` |
| Sydney | Australie | `--city Sydney --country Australia` |
| Montréal | Canada | `--city Montreal --country Canada` |

## Dépannage

### Erreur : "Aucune localisation trouvée"

- Vérifiez l'orthographe de la ville et du pays
- Essayez avec le nom en anglais si le nom français ne fonctionne pas
- Spécifiez le pays pour plus de précision

### Erreur : "Impossible d'obtenir les coordonnées"

- Vérifiez votre connexion Internet
- L'API de géocodage Open-Meteo est gratuite mais peut avoir des limitations
- Attendez quelques secondes et réessayez

### Les informations ville/pays ne sont pas dans les messages transformés

- Vérifiez que vous utilisez la version mise à jour du transformateur
- Assurez-vous que le schéma du transformateur inclut les nouveaux champs
- Redémarrez le transformateur après la mise à jour

## Notes importantes

- **Géocodage gratuit** : L'API Open-Meteo est gratuite et ne nécessite pas de clé API
- **Précision** : Spécifier le pays améliore la précision de la recherche
- **Compatibilité** : Le mode coordonnées (--lat/--lon) reste disponible pour compatibilité
- **Partitionnement** : La clé `country/city` est utilisée pour le partitionnement Kafka et HDFS

## Migration depuis l'exercice 3

Si vous aviez des scripts utilisant les coordonnées, vous pouvez :

1. **Continuer avec les coordonnées** : Utilisez `--lat` et `--lon`
2. **Migrer vers ville/pays** : Remplacez par `--city` et `--country`
3. **Les deux modes sont compatibles** : Les messages avec ou sans ville/pays sont traités correctement
