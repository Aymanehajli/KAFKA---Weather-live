# Exercice 12 : Validation et enrichissement des profils saisonniers

## Description

Cet exercice consiste à valider les profils saisonniers, détecter les valeurs manquantes, vérifier la cohérence des données, et enrichir les profils avec des statistiques de dispersion (écart-type, min, max, médiane, quantiles).

## Prérequis

1. **Services Docker démarrés** (HDFS, Spark)
2. **Données historiques dans HDFS** (voir Exercice 9)
3. **Données sur plusieurs années** pour avoir des statistiques significatives

## Fonctionnalités de validation

### 1. Vérification de complétude

- **12 mois complets** : Vérifie que chaque ville/année possède des données pour les 12 mois
- **Détection des valeurs manquantes** : Identifie les mois manquants pour chaque ville/année
- **Rapport de validation** : Affiche les profils complets et incomplets

### 2. Validation des valeurs réalistes

Les données sont filtrées pour ne garder que les valeurs dans les plages suivantes :
- **Température** : Entre -50°C et +60°C
- **Vitesse du vent** : Entre 0 et 60 m/s

Les valeurs en dehors de ces plages sont exclues du calcul.

## Statistiques de dispersion calculées

### Pour la température

- **std_temperature** : Écart-type de la température moyenne
- **min_temperature** : Température minimale observée
- **max_temperature** : Température maximale observée
- **min_temperature_min** : Température minimale absolue du mois
- **max_temperature_max** : Température maximale absolue du mois

### Pour le vent

- **std_windspeed** : Écart-type de la vitesse du vent moyenne
- **min_windspeed** : Vitesse du vent minimale observée
- **max_windspeed** : Vitesse du vent maximale observée
- **min_windspeed_max** : Rafale minimale du mois
- **max_windspeed_max** : Rafale maximale du mois

## Enrichissement avec quantiles

### Quantiles calculés

- **Q25 (premier quartile)** : 25% des valeurs sont en dessous
- **Médiane (Q50)** : 50% des valeurs sont en dessous
- **Q75 (troisième quartile)** : 75% des valeurs sont en dessous

### Application

- **temp_q25, temp_median, temp_q75** : Quantiles pour la température
- **wind_q25, wind_median, wind_q75** : Quantiles pour le vent

Ces quantiles permettent de définir des seuils dynamiques pour détecter les anomalies basées sur la variabilité, pas seulement sur la moyenne.

## Utilisation

### Option 1 : Script automatique (recommandé)

```bash
./run_profile_enricher.sh
```

### Option 2 : Exécution manuelle

```bash
# 1. Copier le script dans le conteneur
docker cp seasonal_profile_enricher.py pyspark_notebook:/home/jovyan/work/

# 2. Exécuter avec spark-submit
docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --master local[*] \
  /home/jovyan/work/seasonal_profile_enricher.py \
  --hdfs-path /hdfs-data
```

## Options disponibles

- `--hdfs-path` : Chemin HDFS de base (défaut: `/hdfs-data`)

## Structure HDFS

Les profils enrichis sont sauvegardés dans HDFS avec la structure suivante :

```
/hdfs-data/
  ├── France/
  │   └── Paris/
  │       └── seasonal_profile_enriched/
  │           ├── 2014/
  │           │   └── profile.json
  │           ├── 2015/
  │           │   └── profile.json
  │           └── 2024/
  │               └── profile.json
  ├── USA/
  │   └── New_York/
  │       └── seasonal_profile_enriched/
  │           └── 2020/
  │               └── profile.json
```

## Format des données enrichies

### Structure JSON sauvegardée

```json
{
  "city": "Paris",
  "country": "France",
  "year": 2020,
  "latitude": 48.8566,
  "longitude": 2.3522,
  "validation": {
    "months_count": 12,
    "missing_months": [],
    "is_complete": true
  },
  "validation_status": "complete",
  "monthly_profiles": [
    {
      "month": 1,
      "month_name": "Janvier",
      "avg_temperature": 5.2,
      "std_temperature": 2.1,
      "min_temperature": 0.5,
      "max_temperature": 12.3,
      "temp_q25": 3.8,
      "temp_median": 5.1,
      "temp_q75": 6.5,
      "avg_windspeed": 8.5,
      "std_windspeed": 1.2,
      "min_windspeed": 2.1,
      "max_windspeed": 15.3,
      "wind_q25": 7.5,
      "wind_median": 8.4,
      "wind_q75": 9.5,
      "alert_probability": 5.2,
      ...
    },
    ...
  ],
  "computed_at": "2024-01-12T10:30:00.123456",
  "source": "spark-seasonal-enricher"
}
```

## Test complet

### 1. Préparer les données historiques

```bash
# Télécharger 10 ans de données pour Paris
python3 weather_history_loader.py --city Paris --country France --years 10
```

### 2. Exécuter l'enrichisseur

```bash
./run_profile_enricher.sh
```

### 3. Vérifier les profils enrichis dans HDFS

```bash
# Lister la structure
docker exec namenode hdfs dfs -ls -R /hdfs-data/France/Paris/seasonal_profile_enriched/

# Afficher un profil pour une année spécifique
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile_enriched/2020/profile.json | python3 -m json.tool
```

## Exemple de sortie

```
==========================================
📊 Enrichisseur de profils saisonniers
==========================================

📂 Lecture des données historiques depuis HDFS: /hdfs-data
📄 1 fichier(s) historique(s) trouvé(s)
✅ 3650 jour(s) de données chargé(s)

📊 Validation et enrichissement des profils saisonniers...

📊 Calcul des quantiles (médiane, Q25, Q75)...

✅ Validation de la complétude des profils...
✅ Paris, France (2020): Profil complet (12 mois)
✅ Paris, France (2021): Profil complet (12 mois)
...

✅ 10 profil(s) ville/année analysé(s)
✅ 120 profil(s) mensuel(s) enrichi(s)

📋 Aperçu des profils enrichis:
+-------+-----+----+-----+------------------+------------------+------------------+------------------+------------------+------------------+
|country|city |year|month|avg_temperature   |std_temperature   |temp_median       |avg_windspeed     |std_windspeed     |wind_median       |
+-------+-----+----+-----+------------------+------------------+------------------+------------------+------------------+------------------+
|France |Paris|2020|1    |5.2               |2.1               |5.1               |8.5               |1.2               |8.4               |
...
+-------+-----+----+-----+------------------+------------------+------------------+------------------+------------------+------------------+

💾 Sauvegarde des profils enrichis dans HDFS...
✅ Profil sauvegardé: /hdfs-data/France/Paris/seasonal_profile_enriched/2020/profile.json (12 mois)
...

📊 Résumé de validation:
   Profils complets (12 mois): 10
   Profils incomplets: 0
==========================================
✅ Analyse terminée!
==========================================
```

## Utilisation des quantiles pour détecter des anomalies

Les quantiles permettent de détecter des anomalies basées sur la variabilité :

### Exemple : Détection d'une température anormalement élevée

```python
# Si la température d'un jour est > Q75 + 1.5 * (Q75 - Q25)
# C'est une anomalie (méthode des boîtes à moustaches)
iqr = temp_q75 - temp_q25
upper_bound = temp_q75 + 1.5 * iqr

if current_temperature > upper_bound:
    print("Anomalie détectée: température anormalement élevée")
```

### Exemple : Détection d'un vent anormalement faible

```python
# Si la vitesse du vent est < Q25 - 1.5 * (Q75 - Q25)
iqr = wind_q75 - wind_q25
lower_bound = wind_q25 - 1.5 * iqr

if current_windspeed < lower_bound:
    print("Anomalie détectée: vent anormalement faible")
```

## Vérification des profils

### Dans HDFS

```bash
# Lister toutes les années disponibles
docker exec namenode hdfs dfs -ls /hdfs-data/France/Paris/seasonal_profile_enriched/

# Afficher un profil complet
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile_enriched/2020/profile.json | python3 -m json.tool

# Extraire les statistiques de dispersion pour janvier
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile_enriched/2020/profile.json | \
  python3 -c "import json, sys; data=json.load(sys.stdin); \
  jan = [p for p in data['monthly_profiles'] if p['month'] == 1][0]; \
  print(f\"Janvier: {jan['avg_temperature']:.1f}°C (σ={jan['std_temperature']:.1f}, min={jan['min_temperature']:.1f}, max={jan['max_temperature']:.1f})\")"
```

### Interface Web HDFS

Ouvrir `http://localhost:9870` et naviguer vers `/hdfs-data/{country}/{city}/seasonal_profile_enriched/{year}/profile.json`

## Dépannage

### Erreur : "Aucune donnée historique disponible"

Assurez-vous d'avoir exécuté l'exercice 9 :

```bash
python3 weather_history_loader.py --city Paris --country France --years 10
```

### Profils incomplets détectés

- **Cause** : Données manquantes pour certains mois
- **Solution** : Vérifiez que les données historiques couvrent toutes les années complètes
- **Note** : Les profils incomplets sont quand même sauvegardés avec un statut "incomplete"

### Valeurs exclues par validation

- Les valeurs en dehors des plages réalistes sont automatiquement exclues
- Vérifiez les logs pour voir combien de valeurs ont été filtrées
- Si trop de valeurs sont exclues, vérifiez la qualité des données source

## Interprétation des statistiques

### Écart-type (std)

- **Faible** : Faible variabilité, conditions stables
- **Élevé** : Forte variabilité, conditions changeantes

### Quantiles

- **Q25 à Q75** : Plage interquartile (IQR), contient 50% des valeurs
- **Médiane** : Valeur centrale, moins sensible aux valeurs extrêmes que la moyenne
- **Utilisation** : Détection d'anomalies basée sur la variabilité naturelle

### Min/Max

- Montrent les valeurs extrêmes observées
- Utiles pour comprendre la plage de variation réelle

## Notes importantes

- **Validation automatique** : Les valeurs en dehors des plages réalistes sont exclues
- **Profils par année** : Chaque année a son propre profil enrichi
- **Quantiles** : Calculés avec `percentile_approx` pour de meilleures performances
- **Complétude** : Les profils incomplets sont sauvegardés mais marqués comme "incomplete"

## Exemple de workflow complet

```bash
# 1. Télécharger les données historiques
python3 weather_history_loader.py --city Paris --country France --years 10

# 2. Enrichir les profils
./run_profile_enricher.sh

# 3. Vérifier les résultats
docker exec namenode hdfs dfs -ls -R /hdfs-data/France/Paris/seasonal_profile_enriched/

# 4. Analyser un profil spécifique
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile_enriched/2020/profile.json | \
  python3 -m json.tool | head -100
```

## Utilisation pour détection d'anomalies

Les profils enrichis peuvent être utilisés pour détecter des anomalies en temps réel :

```python
# Charger le profil enrichi
with open('profile.json') as f:
    profile = json.load(f)

# Pour un mois donné (ex: janvier)
jan_profile = [p for p in profile['monthly_profiles'] if p['month'] == 1][0]

# Vérifier si une nouvelle valeur est anormale
current_temp = 15.0
iqr = jan_profile['temp_q75'] - jan_profile['temp_q25']
upper_bound = jan_profile['temp_q75'] + 1.5 * iqr
lower_bound = jan_profile['temp_q25'] - 1.5 * iqr

if current_temp > upper_bound or current_temp < lower_bound:
    print(f"Anomalie détectée: {current_temp}°C (plage normale: {lower_bound:.1f} - {upper_bound:.1f}°C)")
```
