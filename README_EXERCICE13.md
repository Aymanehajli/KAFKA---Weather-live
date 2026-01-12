# Exercice 13 : Détection d'anomalies climatiques (Batch vs Speed)

## 📋 Objectif

Implémenter un système de détection d'anomalies climatiques en temps réel qui combine :
- **Speed Layer** : Données temps réel depuis Kafka (`weather_transformed`)
- **Batch Layer** : Profils saisonniers historiques depuis HDFS
- **Jointure Batch vs Speed** : Comparaison en temps réel pour détecter les anomalies

## 🎯 Fonctionnalités

### 1. Ingestion des données

#### Speed Layer (Temps réel)
- Lecture en streaming depuis le topic Kafka `weather_transformed`
- Données météorologiques en temps réel avec alertes

#### Batch Layer (Historique)
- Chargement des profils saisonniers enrichis depuis HDFS
- Structure : `/hdfs-data/{country}/{city}/seasonal_profile_enriched/{year}/profile.json`
- Profils mensuels avec statistiques (moyenne, écart-type, quantiles)

### 2. Jointure Batch vs Speed

**Clé de jointure** : `{country, city, month}`

**Colonnes de référence utilisées** :
- `avg_temperature` : Température moyenne historique
- `avg_windspeed` : Vitesse de vent moyenne historique
- `std_temperature` : Écart-type de la température
- `std_windspeed` : Écart-type du vent
- `alert_probability` : Probabilité historique d'alerte
- Quantiles (Q25, médiane, Q75) pour détection avancée

### 3. Définition des seuils d'anomalie

#### Température
- **Anomalie si** :
  - Écart absolu > **5°C** (configurable)
  - **OU** écart > **2 écarts-types** de la moyenne historique
- **Types d'anomalies** :
  - `heat_wave` : Température anormalement élevée
  - `cold_spell` : Température anormalement basse

#### Vent
- **Anomalie si** :
  - Dépasse la moyenne historique de **2 écarts-types** (configurable)
- **Type d'anomalie** :
  - `wind_storm` : Vent anormalement fort

#### Alertes
- **Anomalie si** :
  - Alerte détectée (level_1 ou level_2) **ET** probabilité historique < 10%
- **Type d'anomalie** :
  - `unexpected_alert` : Alerte inattendue selon l'historique

### 4. Détection en streaming avec Spark

Le job Spark Structured Streaming :
1. Lit en continu depuis `weather_transformed`
2. Joint avec les profils historiques chargés au démarrage
3. Calcule les écarts en temps réel
4. Détecte les anomalies selon les seuils
5. Produit un champ `is_anomaly` (booléen)
6. Produit un champ `anomaly_type` (textuel)

### 5. Métadonnées des anomalies

Chaque anomalie détectée contient :
- `event_time` : Timestamp de l'événement
- `city` : Ville
- `country` : Pays
- `variable` : Type de variable (temperature, windspeed, alert)
- `observed_value` : Valeur observée
- `expected_value` : Valeur attendue (moyenne historique)
- `anomaly_type` : Type d'anomalie (heat_wave, cold_spell, wind_storm, unexpected_alert)

### 6. Publication des anomalies

#### Topic Kafka : `weather_anomalies`

**Format JSON** :
```json
{
  "event_time": "2025-09-23T15:00:00Z",
  "city": "Paris",
  "country": "France",
  "variable": "temperature",
  "observed_value": 30.0,
  "expected_value": 18.0,
  "anomaly_type": "heat_wave"
}
```

### 7. Sauvegarde dans HDFS

**Structure** : `/hdfs-data/{country}/{city}/anomalies/{year}/{month}/anomalies.json`

**Format** :
```json
{
  "country": "France",
  "city": "Paris",
  "year": 2025,
  "month": 9,
  "anomalies": [
    {
      "event_time": "2025-09-23T15:00:00Z",
      "anomaly_type": "heat_wave",
      "observed_temperature": 30.0,
      "expected_temperature": 18.0,
      "temp_deviation": 12.0,
      "observed_windspeed": null,
      "expected_windspeed": null,
      "wind_deviation": null
    }
  ],
  "total_anomalies": 1,
  "updated_at": "2025-09-23T15:05:00Z"
}
```

## 📁 Fichiers créés

### Scripts principaux

1. **`anomaly_detector.py`**
   - Job Spark Structured Streaming
   - Détection d'anomalies en temps réel
   - Jointure Batch vs Speed
   - Publication Kafka + sauvegarde HDFS

2. **`run_anomaly_detector.sh`**
   - Script de lancement automatique
   - Vérification des prérequis
   - Création du topic Kafka
   - Nettoyage des checkpoints
   - Lancement du job Spark

3. **`create_anomalies_topic.sh`**
   - Création du topic Kafka `weather_anomalies`

## 🚀 Utilisation

### Prérequis

1. **Profils saisonniers enrichis** (Exercice 12)
   ```bash
   # Télécharger les données historiques
   python3 weather_history_loader.py --city Paris --country France --years 10
   
   # Générer les profils enrichis
   ./run_profile_enricher.sh
   ```

2. **Topic `weather_transformed` actif**
   - Le producteur météo doit être en cours d'exécution
   - Le transformateur Spark doit être actif

### Démarrage rapide

```bash
# 1. Créer le topic (automatique dans run_anomaly_detector.sh)
./create_anomalies_topic.sh

# 2. Lancer le détecteur d'anomalies
./run_anomaly_detector.sh
```

### Options avancées

```bash
# Lancer avec des seuils personnalisés
docker exec -it pyspark_notebook spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint/anomaly_detector_kafka \
  /tmp/anomaly_detector.py \
  --kafka-servers kafka:9092 \
  --input-topic weather_transformed \
  --output-topic weather_anomalies \
  --hdfs-path /hdfs-data \
  --temp-threshold 7.0 \
  --wind-threshold 2.5
```

### Consommer les anomalies depuis Kafka

```bash
# Consommer les anomalies en temps réel
python3 kafka_consumer.py weather_anomalies
```

### Vérifier les anomalies dans HDFS

```bash
# Lister les anomalies sauvegardées
docker exec namenode hdfs dfs -ls -R /hdfs-data/*/anomalies/

# Afficher les anomalies d'une ville/mois
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/anomalies/2025/09/anomalies.json | python3 -m json.tool
```

## 🔍 Exemples de détection

### Exemple 1 : Vague de chaleur

**Données temps réel** :
- Température : 35°C
- Date : 15 janvier (hiver)

**Profil historique (janvier)** :
- Température moyenne : 5°C
- Écart-type : 3°C

**Détection** :
- Écart : 35 - 5 = 30°C > 5°C ✅
- Écart : 30°C > 2 × 3°C = 6°C ✅
- **Anomalie détectée** : `heat_wave`

### Exemple 2 : Tempête de vent

**Données temps réel** :
- Vent : 25 m/s

**Profil historique (mois actuel)** :
- Vent moyen : 8 m/s
- Écart-type : 4 m/s

**Détection** :
- Écart : 25 - 8 = 17 m/s
- Seuil : 2 × 4 = 8 m/s
- 17 m/s > 8 m/s ✅
- **Anomalie détectée** : `wind_storm`

### Exemple 3 : Alerte inattendue

**Données temps réel** :
- Alerte chaleur : `level_2` (canicule)

**Profil historique (mois actuel)** :
- Probabilité d'alerte : 2%

**Détection** :
- Alerte présente : ✅
- Probabilité historique < 10% : ✅
- **Anomalie détectée** : `unexpected_alert`

## 📊 Architecture

```
┌─────────────────────┐
│  weather_transformed │  ← Speed Layer (Kafka)
│     (Kafka Topic)    │
└──────────┬───────────┘
           │
           │ Streaming
           ▼
┌─────────────────────┐
│  Spark Streaming    │
│  Anomaly Detector   │
└──────────┬──────────┘
           │
           ├─────────────────┐
           │                 │
           ▼                 ▼
┌──────────────────┐  ┌──────────────────────┐
│ seasonal_profiles│  │  Jointure Batch vs   │
│    (HDFS)        │  │       Speed          │
│  Batch Layer     │  │                      │
└──────────────────┘  └──────────┬───────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │  Détection Anomalies   │
                    │  (Seuils configurables)│
                    └──────────┬─────────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
                ▼                             ▼
    ┌──────────────────┐        ┌──────────────────────┐
    │ weather_anomalies│        │  /hdfs-data/.../     │
    │   (Kafka Topic)  │        │  anomalies.json      │
    └──────────────────┘        └──────────────────────┘
```

## ⚙️ Configuration

### Seuils par défaut

- **Température** : ±5°C ou 2 écarts-types
- **Vent** : 2 écarts-types
- **Alertes** : Probabilité historique < 10%

### Personnalisation

Modifiez les arguments dans `run_anomaly_detector.sh` :

```bash
--temp-threshold 7.0    # Seuil température en °C
--wind-threshold 2.5    # Seuil vent en écarts-types
```

## 🔧 Dépannage

### Aucun profil saisonnier trouvé

```
⚠️  Aucun profil saisonnier enrichi trouvé dans HDFS
   Exécutez d'abord l'exercice 12 pour créer les profils enrichis
```

**Solution** :
```bash
# 1. Télécharger les données historiques
python3 weather_history_loader.py --city Paris --country France --years 10

# 2. Générer les profils enrichis
./run_profile_enricher.sh

# 3. Relancer le détecteur
./run_anomaly_detector.sh
```

### Aucune anomalie détectée

**Vérifications** :
1. Le producteur météo est-il actif ?
2. Le transformateur Spark est-il actif ?
3. Les données arrivent-elles dans `weather_transformed` ?
4. Les seuils sont-ils trop stricts ?

**Test** :
```bash
# Consommer weather_transformed pour vérifier les données
python3 kafka_consumer.py weather_transformed

# Vérifier les profils chargés
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile_enriched/2020/profile.json | python3 -m json.tool
```

### Erreur de checkpoint

```
Multiple streaming queries are concurrently using file:/tmp/checkpoint/...
```

**Solution** :
```bash
# Nettoyer les checkpoints
docker exec pyspark_notebook rm -rf /tmp/checkpoint/anomaly_detector_kafka
docker exec pyspark_notebook rm -rf /tmp/checkpoint/anomaly_detector_hdfs

# Relancer
./run_anomaly_detector.sh
```

## 📈 Métriques et monitoring

### Statistiques affichées

Le détecteur affiche en temps réel :
- Nombre de messages traités par batch
- Nombre d'anomalies détectées
- Détails des anomalies (ville, type, écart)

### Exemple de sortie

```
📨 Batch 0: 10 message(s), 2 anomalie(s) détectée(s)
🚨 Anomalies détectées:
+------+-------+-------------+--------------+-------------+
|city  |country|anomaly_type |temp_deviation|wind_deviation|
+------+-------+-------------+--------------+-------------+
|Paris |France |heat_wave    |12.5          |null         |
|Paris |France |wind_storm   |null          |8.3          |
+------+-------+-------------+--------------+-------------+
```

## 🎓 Concepts clés

### Lambda Architecture

- **Speed Layer** : Traitement en temps réel (Kafka + Spark Streaming)
- **Batch Layer** : Données historiques pré-calculées (HDFS)
- **Serving Layer** : Jointure des deux pour enrichir les données temps réel

### Détection d'anomalies

- **Méthode statistique** : Comparaison avec moyenne et écart-type
- **Seuils configurables** : Adaptation selon le contexte
- **Types multiples** : Température, vent, alertes

### Spark Structured Streaming

- **Jointure avec données statiques** : Broadcast des profils historiques
- **Traitement par batch** : Fenêtres de traitement configurables
- **Checkpointing** : Gestion de l'état pour la reprise

## 📚 Références

- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Lambda Architecture](https://en.wikipedia.org/wiki/Lambda_architecture)
- [Anomaly Detection](https://en.wikipedia.org/wiki/Anomaly_detection)

## ✅ Checklist de validation

- [ ] Profils saisonniers enrichis disponibles dans HDFS
- [ ] Topic `weather_transformed` actif avec données
- [ ] Topic `weather_anomalies` créé
- [ ] Détecteur d'anomalies lancé et fonctionnel
- [ ] Anomalies publiées dans Kafka
- [ ] Anomalies sauvegardées dans HDFS
- [ ] Structure HDFS correcte : `/hdfs-data/{country}/{city}/anomalies/{year}/{month}/`

---

**Exercice 13 terminé !** 🎉

Le système de détection d'anomalies est opérationnel et prêt à être utilisé par les dashboards frontend.
