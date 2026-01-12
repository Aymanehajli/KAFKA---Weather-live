# Exercice 1 : Mise en place de Kafka et d'un producteur simple

## Prérequis

Assurez-vous que les services Docker sont démarrés :

```bash
docker-compose up -d
```

Vérifiez que Kafka et Zookeeper sont en cours d'exécution :

```bash
docker ps
```

## Solution

### Option 1 : Script automatique (recommandé)

Exécutez le script complet qui fait tout automatiquement :

```bash
./setup_exercise1.sh
```

### Option 2 : Commandes manuelles

#### 1. Créer le topic `weather_stream`

```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic weather_stream \
  --partitions 1 \
  --replication-factor 1
```

#### 2. Envoyer le message statique

```bash
echo '{"msg": "Hello Kafka"}' | docker exec -i kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic weather_stream
```

## Vérification

Pour vérifier que le message a été envoyé avec succès, vous pouvez utiliser un consommateur :

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather_stream \
  --from-beginning
```

Appuyez sur `Ctrl+C` pour arrêter le consommateur.

## Commandes utiles

- Lister tous les topics :
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

- Décrire un topic :
```bash
docker exec -it kafka kafka-topics --describe --bootstrap-server localhost:9092 --topic weather_stream
```

- Supprimer un topic :
```bash
docker exec -it kafka kafka-topics --delete --bootstrap-server localhost:9092 --topic weather_stream
```
