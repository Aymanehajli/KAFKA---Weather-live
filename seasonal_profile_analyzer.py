#!/usr/bin/env python3
"""
Job Spark pour calculer les profils saisonniers - Exercice 11
Analyse les données historiques et calcule les profils climatiques par mois.
"""

import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, when, sum as spark_sum, month, year,
    expr, collect_list, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
import subprocess
import os


def create_spark_session(app_name="SeasonalProfileAnalyzer"):
    """Crée une session Spark avec les configurations nécessaires."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_historical_data_from_hdfs(spark, hdfs_base_path="/hdfs-data"):
    """
    Lit les données historiques depuis HDFS.
    
    Args:
        spark: SparkSession
        hdfs_base_path: Chemin de base HDFS
    
    Returns:
        DataFrame: Données historiques
    """
    print(f"📂 Lecture des données historiques depuis HDFS: {hdfs_base_path}")
    
    try:
        # Lister tous les fichiers historical_data.json
        result = subprocess.run(
            ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '-R', hdfs_base_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        historical_files = []
        for line in result.stdout.strip().split('\n'):
            if 'weather_history_raw/historical_data.json' in line:
                parts = line.split()
                if len(parts) >= 8:
                    hdfs_path = parts[-1]
                    historical_files.append(hdfs_path)
        
        if not historical_files:
            print("⚠️  Aucun fichier historique trouvé dans HDFS")
            return None
        
        print(f"📄 {len(historical_files)} fichier(s) historique(s) trouvé(s)")
        
        # Lire et parser chaque fichier
        all_data = []
        for hdfs_file in historical_files:
            try:
                result = subprocess.run(
                    ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-cat', hdfs_file],
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                if result.returncode == 0:
                    data = json.loads(result.stdout)
                    
                    # Extraire les métadonnées et les données
                    metadata = data.get('metadata', {})
                    location = metadata.get('location', {})
                    daily_data = data.get('data', {}).get('daily', {})
                    
                    # Créer un DataFrame à partir des données quotidiennes
                    if 'time' in daily_data:
                        times = daily_data.get('time', [])
                        temp_max = daily_data.get('temperature_2m_max', [])
                        temp_min = daily_data.get('temperature_2m_min', [])
                        temp_mean = daily_data.get('temperature_2m_mean', [])
                        precip = daily_data.get('precipitation_sum', [])
                        wind_max = daily_data.get('windspeed_10m_max', [])
                        wind_mean = daily_data.get('windspeed_10m_mean', [])
                        
                        for i in range(len(times)):
                            if times[i]:
                                all_data.append({
                                    'date': times[i],
                                    'city': location.get('city', 'Unknown'),
                                    'country': location.get('country', 'Unknown'),
                                    'latitude': location.get('latitude', 0.0),
                                    'longitude': location.get('longitude', 0.0),
                                    'temperature_max': temp_max[i] if i < len(temp_max) else None,
                                    'temperature_min': temp_min[i] if i < len(temp_min) else None,
                                    'temperature_mean': temp_mean[i] if i < len(temp_mean) else None,
                                    'precipitation': precip[i] if i < len(precip) else None,
                                    'windspeed_max': wind_max[i] if i < len(wind_max) else None,
                                    'windspeed_mean': wind_mean[i] if i < len(wind_mean) else None,
                                })
            except Exception as e:
                print(f"⚠️  Erreur lors de la lecture de {hdfs_file}: {e}", file=sys.stderr)
                continue
        
        if not all_data:
            print("⚠️  Aucune donnée valide trouvée")
            return None
        
        # Créer un DataFrame Spark
        df = spark.createDataFrame(all_data)
        print(f"✅ {df.count()} jour(s) de données chargé(s)")
        return df
        
    except Exception as e:
        print(f"❌ Erreur lors de la lecture HDFS: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return None


def calculate_seasonal_profiles(df):
    """
    Calcule les profils saisonniers par mois pour chaque ville.
    
    Args:
        df: DataFrame avec les données historiques
    
    Returns:
        DataFrame: Profils saisonniers par mois et ville
    """
    print("\n📊 Calcul des profils saisonniers...")
    
    # Filtrer les valeurs nulles
    df_clean = df.filter(
        col('temperature_mean').isNotNull() &
        col('windspeed_mean').isNotNull() &
        col('date').isNotNull()
    )
    
    # Extraire le mois et l'année de la date
    df_with_month = df_clean.withColumn(
        'month',
        expr("month(to_date(date, 'yyyy-MM-dd'))")
    ).withColumn(
        'year',
        expr("year(to_date(date, 'yyyy-MM-dd'))")
    )
    
    # Calculer les niveaux d'alerte pour chaque jour
    # Alerte vent: windspeed >= 10 m/s (level_1) ou > 20 m/s (level_2)
    # Alerte chaleur: température >= 25°C (level_1) ou > 35°C (level_2)
    df_with_alerts = df_with_month.withColumn(
        'wind_alert',
        when(col('windspeed_max') >= 20, 2)  # level_2
        .when(col('windspeed_max') >= 10, 1)  # level_1
        .otherwise(0)  # level_0
    ).withColumn(
        'heat_alert',
        when(col('temperature_max') > 35, 2)  # level_2
        .when(col('temperature_max') >= 25, 1)  # level_1
        .otherwise(0)  # level_0
    ).withColumn(
        'has_alert',
        when((col('wind_alert').isin([1, 2])) | (col('heat_alert').isin([1, 2])), 1)
        .otherwise(0)
    )
    
    # Grouper par ville, pays et mois
    seasonal_profiles = df_with_alerts.groupBy(
        'country', 'city', 'latitude', 'longitude', 'month'
    ).agg(
        # Température moyenne par mois
        avg('temperature_mean').alias('avg_temperature'),
        avg('temperature_max').alias('avg_temperature_max'),
        avg('temperature_min').alias('avg_temperature_min'),
        
        # Vitesse du vent moyenne par mois
        avg('windspeed_mean').alias('avg_windspeed'),
        avg('windspeed_max').alias('avg_windspeed_max'),
        
        # Précipitations moyennes par mois
        avg('precipitation').alias('avg_precipitation'),
        spark_sum('precipitation').alias('total_precipitation'),
        
        # Probabilité d'alerte (nombre de jours avec alerte / total de jours)
        spark_sum('has_alert').alias('days_with_alert'),
        count('*').alias('total_days'),
        
        # Détail des alertes
        spark_sum(when(col('wind_alert') == 1, 1).otherwise(0)).alias('wind_level_1_days'),
        spark_sum(when(col('wind_alert') == 2, 1).otherwise(0)).alias('wind_level_2_days'),
        spark_sum(when(col('heat_alert') == 1, 1).otherwise(0)).alias('heat_level_1_days'),
        spark_sum(when(col('heat_alert') == 2, 1).otherwise(0)).alias('heat_level_2_days'),
    )
    
    # Calculer la probabilité d'alerte
    seasonal_profiles = seasonal_profiles.withColumn(
        'alert_probability',
        (col('days_with_alert') / col('total_days')) * 100
    )
    
    # Ajouter le nom du mois pour la lisibilité
    month_names = {
        1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril',
        5: 'Mai', 6: 'Juin', 7: 'Juillet', 8: 'Août',
        9: 'Septembre', 10: 'Octobre', 11: 'Novembre', 12: 'Décembre'
    }
    
    # Trier par ville et mois
    seasonal_profiles = seasonal_profiles.orderBy('country', 'city', 'month')
    
    return seasonal_profiles


def save_profiles_to_hdfs(profiles_df, hdfs_base_path="/hdfs-data"):
    """
    Sauvegarde les profils saisonniers dans HDFS.
    
    Args:
        profiles_df: DataFrame avec les profils saisonniers
        hdfs_base_path: Chemin de base HDFS
    """
    print("\n💾 Sauvegarde des profils saisonniers dans HDFS...")
    
    # Grouper par ville
    profiles_list = profiles_df.collect()
    
    # Organiser par ville
    city_profiles = {}
    for profile in profiles_list:
        country = profile['country'] or 'Unknown'
        city = profile['city'] or 'Unknown'
        key = f"{country}/{city}"
        
        if key not in city_profiles:
            city_profiles[key] = {
                'city': city,
                'country': country,
                'latitude': profile.get('latitude', 0.0),
                'longitude': profile.get('longitude', 0.0),
                'monthly_profiles': []
            }
        
        city_profiles[key]['monthly_profiles'].append({
            'month': int(profile['month']),
            'month_name': ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin',
                          'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'][int(profile['month']) - 1],
            'avg_temperature': float(profile['avg_temperature']) if profile['avg_temperature'] else None,
            'avg_temperature_max': float(profile['avg_temperature_max']) if profile['avg_temperature_max'] else None,
            'avg_temperature_min': float(profile['avg_temperature_min']) if profile['avg_temperature_min'] else None,
            'avg_windspeed': float(profile['avg_windspeed']) if profile['avg_windspeed'] else None,
            'avg_windspeed_max': float(profile['avg_windspeed_max']) if profile['avg_windspeed_max'] else None,
            'avg_precipitation': float(profile['avg_precipitation']) if profile['avg_precipitation'] else None,
            'total_precipitation': float(profile['total_precipitation']) if profile['total_precipitation'] else None,
            'alert_probability': float(profile['alert_probability']) if profile['alert_probability'] else 0.0,
            'days_with_alert': int(profile['days_with_alert']),
            'total_days': int(profile['total_days']),
            'wind_level_1_days': int(profile['wind_level_1_days']),
            'wind_level_2_days': int(profile['wind_level_2_days']),
            'heat_level_1_days': int(profile['heat_level_1_days']),
            'heat_level_2_days': int(profile['heat_level_2_days'])
        })
    
    # Sauvegarder chaque ville
    for key, profile_data in city_profiles.items():
        country = profile_data['country']
        city = profile_data['city']
        
        country_clean = country.replace('/', '_').replace(' ', '_')
        city_clean = city.replace('/', '_').replace(' ', '_')
        
        hdfs_dir = f"{hdfs_base_path}/{country_clean}/{city_clean}/seasonal_profile"
        hdfs_file = f"{hdfs_dir}/seasonal_profile.json"
        
        # Trier les profils par mois
        profile_data['monthly_profiles'].sort(key=lambda x: x['month'])
        
        try:
            # Créer le répertoire
            subprocess.run(
                ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', hdfs_dir],
                check=True,
                capture_output=True,
                timeout=10
            )
            
            # Ajouter les métadonnées
            profile_data['computed_at'] = datetime.now().isoformat()
            profile_data['source'] = 'spark-seasonal-analyzer'
            
            # Écrire dans un fichier temporaire
            temp_file = f"/tmp/seasonal_profile_{country_clean}_{city_clean}.json"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(profile_data, f, indent=2, ensure_ascii=False)
            
            # Copier dans le conteneur
            subprocess.run(
                ['docker', 'cp', temp_file, f'namenode:/tmp/seasonal_profile_temp.json'],
                check=True,
                capture_output=True,
                timeout=10
            )
            
            # Déplacer dans HDFS
            subprocess.run(
                ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', '-f', '/tmp/seasonal_profile_temp.json', hdfs_file],
                check=True,
                capture_output=True,
                timeout=10
            )
            
            # Nettoyer
            os.remove(temp_file)
            subprocess.run(
                ['docker', 'exec', 'namenode', 'rm', '-f', '/tmp/seasonal_profile_temp.json'],
                capture_output=True
            )
            
            print(f"✅ Profil sauvegardé: {hdfs_file} ({len(profile_data['monthly_profiles'])} mois)")
            
        except Exception as e:
            print(f"⚠️  Erreur lors de la sauvegarde pour {city}, {country}: {e}", file=sys.stderr)


def main():
    """Fonction principale."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Analyseur de profils saisonniers - Calcule les profils climatiques par mois',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  %(prog)s
  %(prog)s --hdfs-path /hdfs-data
        """
    )
    
    parser.add_argument(
        '--hdfs-path',
        default='/hdfs-data',
        help='Chemin HDFS de base (défaut: /hdfs-data)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("📊 Analyseur de profils saisonniers")
    print("=" * 60)
    print()
    
    # Créer la session Spark
    spark = create_spark_session()
    
    try:
        # Lire les données historiques depuis HDFS
        df = read_historical_data_from_hdfs(spark, args.hdfs_path)
        
        if df is None or df.count() == 0:
            print("❌ Aucune donnée historique disponible", file=sys.stderr)
            sys.exit(1)
        
        # Calculer les profils saisonniers
        profiles_df = calculate_seasonal_profiles(df)
        
        if profiles_df is None or profiles_df.count() == 0:
            print("❌ Aucun profil calculé", file=sys.stderr)
            sys.exit(1)
        
        print(f"\n✅ {profiles_df.select('country', 'city').distinct().count()} ville(s) analysée(s)")
        print(f"✅ {profiles_df.count()} profil(s) mensuel(s) calculé(s)")
        
        # Afficher un aperçu
        print("\n📋 Aperçu des profils saisonniers:")
        profiles_df.show(20, truncate=False)
        
        # Sauvegarder dans HDFS
        save_profiles_to_hdfs(profiles_df, args.hdfs_path)
        
        print("\n" + "=" * 60)
        print("✅ Analyse terminée!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Erreur: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
