# Exercice 3 : Streaming de données météo en direct

## Description

Cet exercice consiste à créer un producteur Kafka qui interroge l'API Open-Meteo pour obtenir les données météo actuelles d'une localisation (latitude/longitude) et les envoie au topic Kafka `weather_stream`.

## Prérequis

1. **Python 3.6+** installé sur votre système
2. **Kafka en cours d'exécution** (via Docker Compose)
3. **Bibliothèques Python** installées (voir Installation)

## Installation des dépendances

Installez les dépendances Python nécessaires :

```bash
pip install -r requirements.txt
```

Ou directement :

```bash
pip install kafka-python==2.0.2 requests==2.31.0
```

## API Open-Meteo

L'API Open-Meteo est **gratuite** et **ne nécessite pas de clé API**. Elle fournit des données météo en temps réel pour n'importe quelle localisation sur Terre.

- Documentation : https://open-meteo.com/en/docs
- Endpoint utilisé : `https://api.open-meteo.com/v1/forecast`

## Utilisation

### Syntaxe de base

```bash
python current_weather.py <latitude> <longitude>
```

### Exemples

#### 1. Obtenir la météo pour Paris (48.8566, 2.3522)

```bash
python current_weather.py 48.8566 2.3522
```

#### 2. Obtenir la météo pour New York (40.7128, -74.0060)

```bash
python current_weather.py 40.7128 -74.0060
```

#### 3. Mode continu (streaming en temps réel)

Interroge l'API périodiquement et envoie les données au topic :

```bash
python current_weather.py 48.8566 2.3522 --continuous
```

#### 4. Mode continu avec intervalle personnalisé (toutes les 30 secondes)

```bash
python current_weather.py 48.8566 2.3522 --continuous --interval 30
```

#### 5. Spécifier un topic différent

```bash
python current_weather.py 48.8566 2.3522 --topic my_weather_topic
```

#### 6. Spécifier un serveur Kafka différent

```bash
python current_weather.py 48.8566 2.3522 --bootstrap-servers localhost:29092
```

## Options disponibles

- `latitude` (obligatoire) : Latitude de la localisation (-90 à 90)
- `longitude` (obligatoire) : Longitude de la localisation (-180 à 180)
- `--bootstrap-servers` : Adresse du serveur Kafka (défaut: `localhost:29092`)
- `--topic` : Nom du topic Kafka (défaut: `weather_stream`)
- `--continuous` : Mode continu (interroge l'API périodiquement)
- `--interval` : Intervalle en secondes pour le mode continu (défaut: 60)

## Format des données

Les données envoyées au topic Kafka sont au format JSON et contiennent :

```json
{
  "timestamp": "2024-01-12T10:30:00.123456",
  "location": {
    "latitude": 48.8566,
    "longitude": 2.3522
  },
  "current_weather": {
    "temperature": 15.5,
    "windspeed": 10.2,
    "winddirection": 180,
    "weathercode": 61,
    "time": "2024-01-12T10:00"
  },
  "source": "open-meteo",
  "api_response": { ... }
}
```

### Codes météo (weathercode)

- 0 : Ciel dégagé
- 1-3 : Principalement dégagé, partiellement nuageux, couvert
- 45-48 : Brouillard
- 51-67 : Bruine et pluie
- 71-77 : Neige
- 80-82 : Averses
- 85-86 : Averses de neige
- 95 : Orage
- 96-99 : Orage avec grêle

## Test complet

### 1. Démarrer les services Kafka

```bash
docker-compose up -d
```

### 2. Vérifier que le topic existe

```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

Si le topic n'existe pas, créez-le :

```bash
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic weather_stream \
  --partitions 1 \
  --replication-factor 1
```

### 3. Dans un premier terminal, lancer le consommateur

```bash
python kafka_consumer.py weather_stream --from-beginning
```

### 4. Dans un second terminal, lancer le producteur

**Mode unique :**
```bash
python current_weather.py 48.8566 2.3522
```

**Mode continu :**
```bash
python current_weather.py 48.8566 2.3522 --continuous
```

Le consommateur devrait afficher les données météo en temps réel !

## Exemples de coordonnées

| Ville | Latitude | Longitude |
|-------|----------|-----------|
| Paris | 48.8566 | 2.3522 |
| Londres | 51.5074 | -0.1278 |
| New York | 40.7128 | -74.0060 |
| Tokyo | 35.6762 | 139.6503 |
| Sydney | -33.8688 | 151.2093 |
| Montréal | 45.5017 | -73.5673 |

## Dépannage

### Erreur : "No module named 'requests'"

Installez la dépendance :
```bash
pip install requests
```

### Erreur de connexion à Kafka

Vérifiez que Kafka est bien démarré :
```bash
docker ps | grep kafka
```

Vérifiez le port utilisé (29092 pour l'accès depuis l'hôte).

### Erreur API : "Connection timeout"

- Vérifiez votre connexion Internet
- L'API Open-Meteo est gratuite mais peut avoir des limitations de taux
- Attendez quelques secondes et réessayez

### Aucune donnée reçue par le consommateur

- Vérifiez que le topic est correct : `weather_stream`
- Utilisez `--from-beginning` pour lire les messages historiques
- Vérifiez les logs du producteur pour voir s'il y a des erreurs

## Notes

- Le script utilise le port **29092** par défaut (port mappé pour l'accès depuis l'hôte)
- Le port **9092** est utilisé pour la communication interne entre conteneurs Docker
- L'API Open-Meteo est gratuite et ne nécessite pas d'inscription
- En mode continu, utilisez `Ctrl+C` pour arrêter le producteur
- Les données sont envoyées avec une clé basée sur les coordonnées (format: "lat,lon")
