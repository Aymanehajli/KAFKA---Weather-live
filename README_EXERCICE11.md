# Exercice 11 : Climatologie urbaine (profils saisonniers)

## Description

Cet exercice consiste à créer un job Spark qui analyse les données historiques, regroupe les données par mois pour chaque ville, et calcule les profils saisonniers climatiques.

## Prérequis

1. **Services Docker démarrés** (HDFS, Spark)
2. **Données historiques dans HDFS** (voir Exercice 9)
3. **Données sur plusieurs années** pour avoir des statistiques significatives par mois

## Métriques calculées

Pour chaque ville et chaque mois, le job calcule :

### 1. Température moyenne par mois (profil saisonnier)

- **avg_temperature** : Température moyenne mensuelle
- **avg_temperature_max** : Température maximale moyenne mensuelle
- **avg_temperature_min** : Température minimale moyenne mensuelle

### 2. Vitesse du vent moyenne

- **avg_windspeed** : Vitesse du vent moyenne mensuelle
- **avg_windspeed_max** : Vitesse du vent maximale moyenne mensuelle

### 3. Probabilité d'alerte

- **alert_probability** : Pourcentage de jours avec alerte (level_1 ou level_2) par mois
- **days_with_alert** : Nombre de jours avec alerte
- **total_days** : Nombre total de jours dans le mois (sur toutes les années)

### 4. Détails des alertes

- **wind_level_1_days** : Nombre de jours avec vent modéré (10-20 m/s)
- **wind_level_2_days** : Nombre de jours avec vent fort (> 20 m/s)
- **heat_level_1_days** : Nombre de jours avec chaleur modérée (25-35°C)
- **heat_level_2_days** : Nombre de jours avec canicule (> 35°C)

### 5. Précipitations

- **avg_precipitation** : Précipitations moyennes quotidiennes par mois
- **total_precipitation** : Précipitations totales mensuelles

## Utilisation

### Option 1 : Script automatique (recommandé)

```bash
./run_seasonal_analyzer.sh
```

### Option 2 : Exécution manuelle

```bash
# 1. Copier le script dans le conteneur
docker cp seasonal_profile_analyzer.py pyspark_notebook:/home/jovyan/work/

# 2. Exécuter avec spark-submit
docker exec -it pyspark_notebook \
  /usr/local/spark/bin/spark-submit \
  --master local[*] \
  /home/jovyan/work/seasonal_profile_analyzer.py \
  --hdfs-path /hdfs-data
```

## Options disponibles

- `--hdfs-path` : Chemin HDFS de base (défaut: `/hdfs-data`)

## Structure HDFS

Les profils saisonniers sont sauvegardés dans HDFS avec la structure suivante :

```
/hdfs-data/
  ├── France/
  │   └── Paris/
  │       └── seasonal_profile/
  │           └── seasonal_profile.json
  ├── USA/
  │   └── New_York/
  │       └── seasonal_profile/
  │           └── seasonal_profile.json
  └── UK/
      └── London/
          └── seasonal_profile/
              └── seasonal_profile.json
```

## Format des données

### Structure JSON sauvegardée

```json
{
  "city": "Paris",
  "country": "France",
  "latitude": 48.8566,
  "longitude": 2.3522,
  "monthly_profiles": [
    {
      "month": 1,
      "month_name": "Janvier",
      "avg_temperature": 5.2,
      "avg_temperature_max": 7.8,
      "avg_temperature_min": 2.6,
      "avg_windspeed": 8.5,
      "avg_windspeed_max": 12.3,
      "avg_precipitation": 2.1,
      "total_precipitation": 65.1,
      "alert_probability": 5.2,
      "days_with_alert": 16,
      "total_days": 310,
      "wind_level_1_days": 12,
      "wind_level_2_days": 2,
      "heat_level_1_days": 0,
      "heat_level_2_days": 0
    },
    {
      "month": 2,
      "month_name": "Février",
      ...
    },
    ...
  ],
  "computed_at": "2024-01-12T10:30:00.123456",
  "source": "spark-seasonal-analyzer"
}
```

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
./run_seasonal_analyzer.sh
```

### 3. Vérifier les profils dans HDFS

```bash
# Lister la structure
docker exec namenode hdfs dfs -ls -R /hdfs-data/France/Paris/

# Afficher le profil saisonnier
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile/seasonal_profile.json | python3 -m json.tool
```

## Exemple de sortie

```
==========================================
📊 Analyseur de profils saisonniers
==========================================

📂 Lecture des données historiques depuis HDFS: /hdfs-data
📄 1 fichier(s) historique(s) trouvé(s)
✅ 3650 jour(s) de données chargé(s)

📊 Calcul des profils saisonniers...

✅ 1 ville(s) analysée(s)
✅ 12 profil(s) mensuel(s) calculé(s)

📋 Aperçu des profils saisonniers:
+-------+-----+-----+------------------+------------------+------------------+------------------+
|country|city |month|avg_temperature   |avg_windspeed     |alert_probability |total_days        |
+-------+-----+-----+------------------+------------------+------------------+------------------+
|France |Paris|1    |5.2               |8.5               |5.2               |310               |
|France |Paris|2    |6.1               |9.2               |4.8               |280               |
...
+-------+-----+-----+------------------+------------------+------------------+------------------+

💾 Sauvegarde des profils saisonniers dans HDFS...
✅ Profil sauvegardé: /hdfs-data/France/Paris/seasonal_profile/seasonal_profile.json (12 mois)

==========================================
✅ Analyse terminée!
==========================================
```

## Interprétation des résultats

### Profil saisonnier de température

Le profil montre l'évolution de la température au cours de l'année :
- **Hiver** (décembre-février) : Températures les plus basses
- **Printemps** (mars-mai) : Températures en hausse
- **Été** (juin-août) : Températures les plus élevées
- **Automne** (septembre-novembre) : Températures en baisse

### Probabilité d'alerte

- **Été** : Probabilité d'alerte de chaleur élevée
- **Hiver** : Probabilité d'alerte de vent élevée (selon la région)
- **Printemps/Automne** : Probabilités généralement plus faibles

### Vitesse du vent

- Varie selon les saisons et les régions
- Généralement plus élevée en hiver dans les régions tempérées

## Vérification des profils

### Dans HDFS

```bash
# Afficher le profil complet
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile/seasonal_profile.json | python3 -m json.tool

# Extraire uniquement les températures moyennes
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile/seasonal_profile.json | \
  python3 -c "import json, sys; data=json.load(sys.stdin); \
  [print(f\"{p['month_name']}: {p['avg_temperature']:.1f}°C\") for p in data['monthly_profiles']]"
```

### Interface Web HDFS

Ouvrir `http://localhost:9870` et naviguer vers `/hdfs-data/{country}/{city}/seasonal_profile/seasonal_profile.json`

## Utilisation pour visualisation

Les profils saisonniers peuvent être utilisés pour créer des graphiques :

```python
import json
import matplotlib.pyplot as plt

# Charger le profil
with open('seasonal_profile.json') as f:
    profile = json.load(f)

months = [p['month_name'] for p in profile['monthly_profiles']]
temps = [p['avg_temperature'] for p in profile['monthly_profiles']]

plt.plot(months, temps, marker='o')
plt.title(f"Profil saisonnier - {profile['city']}")
plt.xlabel('Mois')
plt.ylabel('Température moyenne (°C)')
plt.xticks(rotation=45)
plt.grid(True)
plt.show()
```

## Dépannage

### Erreur : "Aucune donnée historique disponible"

Assurez-vous d'avoir exécuté l'exercice 9 :

```bash
python3 weather_history_loader.py --city Paris --country France --years 10
```

### Erreur : "Aucun profil calculé"

- Vérifiez que les données contiennent des dates valides
- Vérifiez que les données ne contiennent pas trop de valeurs nulles
- Assurez-vous d'avoir des données sur plusieurs années pour chaque mois

### Les probabilités d'alerte sont à 0

- Vérifiez que les données contiennent des valeurs de température et vent suffisamment élevées
- Les seuils d'alerte sont : vent >= 10 m/s, température >= 25°C

## Notes importantes

- **Agrégation par mois** : Les données de toutes les années sont regroupées par mois
- **Probabilité** : Calculée comme (jours avec alerte / total jours) × 100
- **Données manquantes** : Les jours avec valeurs nulles sont exclus des calculs
- **Format** : Les profils sont sauvegardés en JSON dans HDFS

## Exemple de workflow complet

```bash
# 1. Télécharger les données historiques
python3 weather_history_loader.py --city Paris --country France --years 10

# 2. Calculer les profils saisonniers
./run_seasonal_analyzer.sh

# 3. Visualiser les résultats
docker exec namenode hdfs dfs -cat /hdfs-data/France/Paris/seasonal_profile/seasonal_profile.json | \
  python3 -m json.tool | head -50
```
