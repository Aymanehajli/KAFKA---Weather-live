# Exercice 2 : Écriture d'un consommateur Kafka

## Description

Cet exercice consiste à créer un script Python consommateur qui lit les messages depuis un topic Kafka et les affiche en temps réel.

## Prérequis

1. **Python 3.6+** installé sur votre système
2. **Kafka en cours d'exécution** (via Docker Compose)
3. **Bibliothèque kafka-python** installée

## Installation des dépendances

Installez les dépendances Python nécessaires :

```bash
pip install -r requirements.txt
```

Ou directement :

```bash
pip install kafka-python==2.0.2
```

## Utilisation

### Syntaxe de base

```bash
python kafka_consumer.py <topic>
```

### Exemples

#### 1. Consommer depuis le topic `weather_stream`

```bash
python kafka_consumer.py weather_stream
```

#### 2. Lire depuis le début du topic (tous les messages historiques)

```bash
python kafka_consumer.py weather_stream --from-beginning
```

#### 3. Spécifier un serveur Kafka différent

```bash
python kafka_consumer.py weather_stream --bootstrap-servers localhost:29092
```

#### 4. Utiliser un groupe de consommateurs

```bash
python kafka_consumer.py weather_stream --group-id my-consumer-group
```

#### 5. Combinaison d'options

```bash
python kafka_consumer.py weather_stream --from-beginning --group-id weather-consumers
```

## Options disponibles

- `topic` (obligatoire) : Nom du topic Kafka à consommer
- `--bootstrap-servers` : Adresse du serveur Kafka (défaut: `localhost:29092`)
- `--group-id` : ID du groupe de consommateurs (optionnel)
- `--from-beginning` : Lire les messages depuis le début du topic

## Fonctionnalités

- ✅ Lecture en temps réel des messages
- ✅ Affichage formaté avec informations du message (topic, partition, offset, timestamp)
- ✅ Détection automatique du format JSON
- ✅ Gestion propre de l'arrêt (Ctrl+C)
- ✅ Support des groupes de consommateurs
- ✅ Options de configuration flexibles

## Test complet

### 1. Démarrer les services Kafka

```bash
docker-compose up -d
```

### 2. Créer et envoyer un message (si pas déjà fait)

```bash
./setup_exercise1.sh
```

### 3. Dans un premier terminal, démarrer le consommateur

```bash
python kafka_consumer.py weather_stream --from-beginning
```

### 4. Dans un second terminal, envoyer un nouveau message

```bash
echo '{"msg": "Test en temps réel", "timestamp": "'$(date)'"}' | \
  docker exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic weather_stream
```

Le consommateur devrait afficher le nouveau message en temps réel !

## Dépannage

### Erreur : "No module named 'kafka'"

Installez la dépendance :
```bash
pip install kafka-python
```

### Erreur de connexion

Vérifiez que Kafka est bien démarré :
```bash
docker ps | grep kafka
```

Vérifiez le port utilisé (29092 pour l'accès depuis l'hôte) :
```bash
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Aucun message affiché

- Utilisez `--from-beginning` pour lire les messages historiques
- Vérifiez que des messages ont bien été envoyés au topic
- Vérifiez le nom du topic avec : `docker exec kafka kafka-topics --list --bootstrap-server localhost:9092`

## Notes

- Le script utilise le port **29092** par défaut (port mappé pour l'accès depuis l'hôte)
- Le port **9092** est utilisé pour la communication interne entre conteneurs Docker
- Pour arrêter le consommateur, utilisez `Ctrl+C`
