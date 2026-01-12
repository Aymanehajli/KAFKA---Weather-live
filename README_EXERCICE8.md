# Exercice 8 : Visualisation et agrégation des logs météo

## Description

Cet exercice consiste à créer un système de visualisation qui consomme les logs depuis HDFS (ou Kafka) et génère des graphiques pour analyser les données météo.

## Prérequis

1. **Services Docker démarrés** (Kafka, HDFS)
2. **Données disponibles** dans HDFS ou Kafka
3. **Bibliothèques Python** installées (matplotlib, seaborn, pandas)

## Installation des dépendances

```bash
pip install -r requirements.txt
```

Ou directement :

```bash
pip install matplotlib seaborn pandas
```

## Visualisations implémentées

### 1. Évolution de la température au fil du temps

- Graphique linéaire montrant l'évolution de la température
- Séparation par ville si plusieurs villes sont présentes
- Axe temporel formaté avec dates et heures

### 2. Évolution de la vitesse du vent au fil du temps

- Graphique linéaire montrant l'évolution de la vitesse du vent
- Séparation par ville si plusieurs villes sont présentes
- Axe temporel formaté avec dates et heures

### 3. Nombre d'alertes vent et chaleur par niveau

- Graphique en barres pour les alertes de vent (level_1, level_2)
- Graphique en barres pour les alertes de chaleur (level_1, level_2)
- Codes couleur : orange pour level_1, rouge pour level_2

### 4. Code météo le plus fréquent par pays

- Graphique en barres horizontales montrant le code météo le plus fréquent par pays
- Heatmap montrant la distribution complète des codes météo par pays
- Permet d'identifier les conditions météo dominantes par région

## Utilisation

### Source HDFS (par défaut)

```bash
python3 weather_visualizer.py --source hdfs
```

### Source Kafka

```bash
python3 weather_visualizer.py --source kafka --kafka-limit 1000
```

### Options disponibles

- `--source` : Source des données (`hdfs` ou `kafka`, défaut: `hdfs`)
- `--hdfs-path` : Chemin HDFS de base (défaut: `/hdfs-data`)
- `--kafka-topic` : Topic Kafka (défaut: `weather_transformed`)
- `--kafka-limit` : Nombre maximum de messages depuis Kafka (défaut: 1000)
- `--output-dir` : Répertoire de sortie pour les graphiques (défaut: `output`)

### Exemples

#### Visualisation depuis HDFS

```bash
python3 weather_visualizer.py --source hdfs --hdfs-path /hdfs-data
```

#### Visualisation depuis Kafka avec limite

```bash
python3 weather_visualizer.py --source kafka --kafka-limit 500
```

#### Spécifier un répertoire de sortie personnalisé

```bash
python3 weather_visualizer.py --output-dir visualizations
```

## Fichiers générés

Le script génère 4 fichiers PNG dans le répertoire de sortie :

1. **`temperature_evolution.png`** : Évolution de la température
2. **`windspeed_evolution.png`** : Évolution de la vitesse du vent
3. **`alerts_by_level.png`** : Nombre d'alertes par niveau
4. **`weathercode_by_country.png`** : Codes météo par pays

## Test complet

### 1. Démarrer les services

```bash
docker-compose up -d
```

### 2. Générer des données

```bash
# Terminal 1: Producteur
python3 current_weather.py --city Paris --country France --continuous --interval 30

# Terminal 2: Transformateur
./run_weather_transformer.sh

# Terminal 3: Sauvegarde HDFS (optionnel, pour avoir des données dans HDFS)
python3 hdfs_alert_saver.py
```

### 3. Générer les visualisations

```bash
# Depuis HDFS (alertes uniquement)
python3 weather_visualizer.py --source hdfs

# Depuis Kafka (toutes les données transformées)
python3 weather_visualizer.py --source kafka --kafka-limit 500
```

### 4. Visualiser les graphiques

Les graphiques sont sauvegardés dans le répertoire `output/`. Ouvrez-les avec votre visualiseur d'images préféré.

## Détails des visualisations

### Évolution de la température

- **Type** : Graphique linéaire
- **Axe X** : Temps (formaté avec dates et heures)
- **Axe Y** : Température en °C
- **Légende** : Une ligne par ville (si plusieurs villes)

### Évolution de la vitesse du vent

- **Type** : Graphique linéaire
- **Axe X** : Temps (formaté avec dates et heures)
- **Axe Y** : Vitesse du vent en m/s
- **Légende** : Une ligne par ville (si plusieurs villes)

### Alertes par niveau

- **Type** : Graphiques en barres (2 sous-graphiques)
- **Gauche** : Alertes de vent (level_1, level_2)
- **Droite** : Alertes de chaleur (level_1, level_2)
- **Couleurs** : Orange (level_1), Rouge (level_2)

### Codes météo par pays

- **Type** : Barres horizontales + Heatmap
- **Gauche** : Code météo le plus fréquent par pays
- **Droite** : Distribution complète des codes météo (heatmap)
- **Couleurs** : Échelle de couleurs pour la fréquence

## Codes météo (référence)

- **0** : Ciel dégagé
- **1-3** : Principalement dégagé, partiellement nuageux, couvert
- **45-48** : Brouillard
- **51-67** : Bruine et pluie
- **71-77** : Neige
- **80-82** : Averses
- **85-86** : Averses de neige
- **95** : Orage
- **96-99** : Orage avec grêle

## Dépannage

### Erreur : "No module named 'matplotlib'"

Installez les dépendances :
```bash
pip install matplotlib seaborn pandas
```

### Aucune donnée chargée depuis HDFS

- Vérifiez que des alertes ont été sauvegardées dans HDFS
- Vérifiez le chemin HDFS : `docker exec namenode hdfs dfs -ls -R /hdfs-data`
- Utilisez `--source kafka` pour lire depuis Kafka à la place

### Aucune donnée chargée depuis Kafka

- Vérifiez que le topic `weather_transformed` contient des messages
- Vérifiez que Kafka est accessible : `docker ps | grep kafka`
- Augmentez `--kafka-limit` si nécessaire

### Les graphiques sont vides

- Vérifiez que les données contiennent les champs nécessaires
- Vérifiez les logs pour voir combien de données ont été chargées
- Assurez-vous que les données ont des timestamps valides

### Erreur de format de date

- Les données doivent avoir un champ `event_time` au format ISO
- Vérifiez que les timestamps sont valides dans les données source

## Personnalisation

### Modifier les couleurs

Éditez le script `weather_visualizer.py` et modifiez les paramètres `color` dans les fonctions de création de graphiques.

### Ajouter de nouvelles visualisations

Ajoutez une nouvelle fonction dans `weather_visualizer.py` et appelez-la dans la fonction `main()`.

### Changer la résolution des graphiques

Modifiez le paramètre `dpi` dans les appels `plt.savefig()` (défaut: 300).

## Exemple de workflow complet

```bash
# 1. Générer des données
python3 current_weather.py --city Paris --country France --continuous &
./run_weather_transformer.sh &

# 2. Attendre quelques minutes pour accumuler des données

# 3. Générer les visualisations
python3 weather_visualizer.py --source kafka --kafka-limit 200

# 4. Visualiser les résultats
ls -lh output/
open output/*.png  # Sur macOS
```

## Notes importantes

- **Source HDFS** : Lit uniquement les alertes (messages avec level_1 ou level_2)
- **Source Kafka** : Lit toutes les données transformées (plus de données disponibles)
- **Format de sortie** : PNG haute résolution (300 DPI)
- **Performance** : Pour de gros volumes, utilisez `--kafka-limit` pour limiter les données
