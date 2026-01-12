#!/usr/bin/env python3
"""
Job Spark pour valider et enrichir les profils saisonniers - Exercice 12
Valide les profils, calcule des statistiques de dispersion et enrichit avec médiane et quantiles.
"""

import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, when, sum as spark_sum, month, year,
    expr, stddev, min as spark_min, max as spark_max,
    percentile_approx as pct_approx, collect_list, struct, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
import subprocess
import os


def create_spark_session(app_name="SeasonalProfileEnricher"):
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
        DataFrame: Données historiques avec année
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
                                # Extraire l'année
                                date_parts = times[i].split('-')
                                year_val = int(date_parts[0]) if len(date_parts) > 0 else None
                                
                                all_data.append({
                                    'date': times[i],
                                    'year': year_val,
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


def validate_and_enrich_profiles(df):
    """
    Valide et enrichit les profils saisonniers avec statistiques de dispersion.
    
    Args:
        df: DataFrame avec les données historiques
    
    Returns:
        DataFrame: Profils enrichis par ville, année et mois
    """
    print("\n📊 Validation et enrichissement des profils saisonniers...")
    
    # Filtrer les valeurs nulles et valider les plages réalistes
    df_clean = df.filter(
        col('temperature_mean').isNotNull() &
        col('windspeed_mean').isNotNull() &
        col('date').isNotNull() &
        col('year').isNotNull() &
        # Validation des plages réalistes
        (col('temperature_mean') >= -50) & (col('temperature_mean') <= 60) &
        (col('windspeed_max') >= 0) & (col('windspeed_max') <= 60) &
        (col('windspeed_mean') >= 0) & (col('windspeed_mean') <= 60)
    )
    
    # Extraire le mois de la date
    df_with_month = df_clean.withColumn(
        'month',
        expr("month(to_date(date, 'yyyy-MM-dd'))")
    )
    
    # Calculer les niveaux d'alerte
    df_with_alerts = df_with_month.withColumn(
        'wind_alert',
        when(col('windspeed_max') >= 20, 2)
        .when(col('windspeed_max') >= 10, 1)
        .otherwise(0)
    ).withColumn(
        'heat_alert',
        when(col('temperature_max') > 35, 2)
        .when(col('temperature_max') >= 25, 1)
        .otherwise(0)
    ).withColumn(
        'has_alert',
        when((col('wind_alert').isin([1, 2])) | (col('heat_alert').isin([1, 2])), 1)
        .otherwise(0)
    )
    
    # Grouper par ville, pays, année et mois
    enriched_profiles = df_with_alerts.groupBy(
        'country', 'city', 'latitude', 'longitude', 'year', 'month'
    ).agg(
        # Température - statistiques de base
        avg('temperature_mean').alias('avg_temperature'),
        avg('temperature_max').alias('avg_temperature_max'),
        avg('temperature_min').alias('avg_temperature_min'),
        
        # Température - statistiques de dispersion
        stddev('temperature_mean').alias('std_temperature'),
        spark_min('temperature_mean').alias('min_temperature'),
        spark_max('temperature_mean').alias('max_temperature'),
        spark_min('temperature_min').alias('min_temperature_min'),
        spark_max('temperature_max').alias('max_temperature_max'),
        
        # Vent - statistiques de base
        avg('windspeed_mean').alias('avg_windspeed'),
        avg('windspeed_max').alias('avg_windspeed_max'),
        
        # Vent - statistiques de dispersion
        stddev('windspeed_mean').alias('std_windspeed'),
        spark_min('windspeed_mean').alias('min_windspeed'),
        spark_max('windspeed_mean').alias('max_windspeed'),
        spark_min('windspeed_max').alias('min_windspeed_max'),
        spark_max('windspeed_max').alias('max_windspeed_max'),
        
        # Précipitations
        avg('precipitation').alias('avg_precipitation'),
        spark_sum('precipitation').alias('total_precipitation'),
        
        # Alertes
        spark_sum('has_alert').alias('days_with_alert'),
        count('*').alias('total_days'),
        spark_sum(when(col('wind_alert') == 1, 1).otherwise(0)).alias('wind_level_1_days'),
        spark_sum(when(col('wind_alert') == 2, 1).otherwise(0)).alias('wind_level_2_days'),
        spark_sum(when(col('heat_alert') == 1, 1).otherwise(0)).alias('heat_level_1_days'),
        spark_sum(when(col('heat_alert') == 2, 1).otherwise(0)).alias('heat_level_2_days'),
    )
    
    # Calculer la probabilité d'alerte
    enriched_profiles = enriched_profiles.withColumn(
        'alert_probability',
        (col('days_with_alert') / col('total_days')) * 100
    )
    
    # Pour calculer médiane et quantiles, on doit collecter les valeurs
    # Note: percentile_approx nécessite une valeur numérique, on utilisera une approche différente
    # On va calculer les quantiles en collectant les valeurs par groupe
    
    return enriched_profiles


def calculate_quantiles(df_with_month, enriched_profiles):
    """
    Calcule les quantiles (médiane, Q25, Q75) pour chaque groupe.
    
    Args:
        df_with_month: DataFrame avec les données quotidiennes
        enriched_profiles: DataFrame avec les profils enrichis
    
    Returns:
        DataFrame: Profils avec quantiles ajoutés
    """
    print("\n📊 Calcul des quantiles (médiane, Q25, Q75)...")
    
    # Filtrer les valeurs nulles pour les quantiles
    df_clean = df_with_month.filter(
        col('temperature_mean').isNotNull() &
        col('windspeed_mean').isNotNull()
    )
    
    # Calculer les quantiles pour la température et le vent
    # Note: percentile_approx nécessite une colonne numérique
    temp_quantiles = df_clean.groupBy('country', 'city', 'year', 'month').agg(
        pct_approx('temperature_mean', 0.25).alias('temp_q25'),
        pct_approx('temperature_mean', 0.5).alias('temp_median'),
        pct_approx('temperature_mean', 0.75).alias('temp_q75'),
        pct_approx('windspeed_mean', 0.25).alias('wind_q25'),
        pct_approx('windspeed_mean', 0.5).alias('wind_median'),
        pct_approx('windspeed_mean', 0.75).alias('wind_q75'),
    )
    
    # Joindre avec les profils enrichis
    final_profiles = enriched_profiles.join(
        temp_quantiles,
        ['country', 'city', 'year', 'month'],
        'left'
    )
    
    return final_profiles


def validate_profile_completeness(profiles_df):
    """
    Valide que chaque ville/pays possède un profil complet sur 12 mois.
    
    Args:
        profiles_df: DataFrame avec les profils
    
    Returns:
        dict: Résultats de validation par ville
    """
    print("\n✅ Validation de la complétude des profils...")
    
    validation_results = {}
    
    # Grouper par ville et année
    city_years = profiles_df.select('country', 'city', 'year').distinct().collect()
    
    for city_year in city_years:
        country = city_year['country']
        city = city_year['city']
        year = city_year['year']
        key = f"{country}/{city}/{year}"
        
        # Compter les mois disponibles
        months = profiles_df.filter(
            (col('country') == country) &
            (col('city') == city) &
            (col('year') == year)
        ).select('month').distinct().collect()
        
        month_numbers = [m['month'] for m in months]
        missing_months = [m for m in range(1, 13) if m not in month_numbers]
        
        validation_results[key] = {
            'country': country,
            'city': city,
            'year': year,
            'months_count': len(month_numbers),
            'missing_months': missing_months,
            'is_complete': len(month_numbers) == 12
        }
        
        if len(month_numbers) == 12:
            print(f"✅ {city}, {country} ({year}): Profil complet (12 mois)")
        else:
            print(f"⚠️  {city}, {country} ({year}): {len(month_numbers)}/12 mois - Manquants: {missing_months}")
    
    return validation_results


def save_enriched_profiles_to_hdfs(profiles_df, validation_results, hdfs_base_path="/hdfs-data"):
    """
    Sauvegarde les profils enrichis dans HDFS par année.
    
    Args:
        profiles_df: DataFrame avec les profils enrichis
        validation_results: Résultats de validation
        hdfs_base_path: Chemin de base HDFS
    """
    print("\n💾 Sauvegarde des profils enrichis dans HDFS...")
    
    # Organiser par ville et année
    profiles_list = profiles_df.orderBy('country', 'city', 'year', 'month').collect()
    
    city_year_profiles = {}
    for profile in profiles_list:
        country = profile['country'] or 'Unknown'
        city = profile['city'] or 'Unknown'
        year = profile['year']
        key = f"{country}/{city}/{year}"
        
        if key not in city_year_profiles:
            city_year_profiles[key] = {
                'city': city,
                'country': country,
                'year': int(year),
                'latitude': profile.get('latitude', 0.0),
                'longitude': profile.get('longitude', 0.0),
                'monthly_profiles': [],
                'validation': validation_results.get(key, {})
            }
        
        # Créer le profil mensuel enrichi
        monthly_profile = {
            'month': int(profile['month']),
            'month_name': ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin',
                          'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'][int(profile['month']) - 1],
            # Température - moyennes
            'avg_temperature': float(profile['avg_temperature']) if profile['avg_temperature'] else None,
            'avg_temperature_max': float(profile['avg_temperature_max']) if profile['avg_temperature_max'] else None,
            'avg_temperature_min': float(profile['avg_temperature_min']) if profile['avg_temperature_min'] else None,
            # Température - dispersion
            'std_temperature': float(profile['std_temperature']) if profile['std_temperature'] else None,
            'min_temperature': float(profile['min_temperature']) if profile['min_temperature'] else None,
            'max_temperature': float(profile['max_temperature']) if profile['max_temperature'] else None,
            'min_temperature_min': float(profile['min_temperature_min']) if profile['min_temperature_min'] else None,
            'max_temperature_max': float(profile['max_temperature_max']) if profile['max_temperature_max'] else None,
            # Température - quantiles
            'temp_q25': float(profile['temp_q25']) if profile.get('temp_q25') else None,
            'temp_median': float(profile['temp_median']) if profile.get('temp_median') else None,
            'temp_q75': float(profile['temp_q75']) if profile.get('temp_q75') else None,
            # Vent - moyennes
            'avg_windspeed': float(profile['avg_windspeed']) if profile['avg_windspeed'] else None,
            'avg_windspeed_max': float(profile['avg_windspeed_max']) if profile['avg_windspeed_max'] else None,
            # Vent - dispersion
            'std_windspeed': float(profile['std_windspeed']) if profile['std_windspeed'] else None,
            'min_windspeed': float(profile['min_windspeed']) if profile['min_windspeed'] else None,
            'max_windspeed': float(profile['max_windspeed']) if profile['max_windspeed'] else None,
            'min_windspeed_max': float(profile['min_windspeed_max']) if profile['min_windspeed_max'] else None,
            'max_windspeed_max': float(profile['max_windspeed_max']) if profile['max_windspeed_max'] else None,
            # Vent - quantiles
            'wind_q25': float(profile['wind_q25']) if profile.get('wind_q25') else None,
            'wind_median': float(profile['wind_median']) if profile.get('wind_median') else None,
            'wind_q75': float(profile['wind_q75']) if profile.get('wind_q75') else None,
            # Précipitations
            'avg_precipitation': float(profile['avg_precipitation']) if profile['avg_precipitation'] else None,
            'total_precipitation': float(profile['total_precipitation']) if profile['total_precipitation'] else None,
            # Alertes
            'alert_probability': float(profile['alert_probability']) if profile['alert_probability'] else 0.0,
            'days_with_alert': int(profile['days_with_alert']),
            'total_days': int(profile['total_days']),
            'wind_level_1_days': int(profile['wind_level_1_days']),
            'wind_level_2_days': int(profile['wind_level_2_days']),
            'heat_level_1_days': int(profile['heat_level_1_days']),
            'heat_level_2_days': int(profile['heat_level_2_days'])
        }
        
        city_year_profiles[key]['monthly_profiles'].append(monthly_profile)
    
    # Sauvegarder chaque ville/année
    for key, profile_data in city_year_profiles.items():
        country = profile_data['country']
        city = profile_data['city']
        year = profile_data['year']
        
        country_clean = country.replace('/', '_').replace(' ', '_')
        city_clean = city.replace('/', '_').replace(' ', '_')
        
        hdfs_dir = f"{hdfs_base_path}/{country_clean}/{city_clean}/seasonal_profile_enriched/{year}"
        hdfs_file = f"{hdfs_dir}/profile.json"
        
        # Trier les profils par mois
        profile_data['monthly_profiles'].sort(key=lambda x: x['month'])
        
        # Ajouter les métadonnées
        profile_data['computed_at'] = datetime.now().isoformat()
        profile_data['source'] = 'spark-seasonal-enricher'
        profile_data['validation_status'] = 'complete' if profile_data['validation'].get('is_complete') else 'incomplete'
        
        try:
            # Créer le répertoire
            subprocess.run(
                ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', hdfs_dir],
                check=True,
                capture_output=True,
                timeout=10
            )
            
            # Écrire dans un fichier temporaire
            temp_file = f"/tmp/seasonal_profile_enriched_{country_clean}_{city_clean}_{year}.json"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(profile_data, f, indent=2, ensure_ascii=False)
            
            # Copier dans le conteneur
            subprocess.run(
                ['docker', 'cp', temp_file, f'namenode:/tmp/seasonal_profile_enriched_temp.json'],
                check=True,
                capture_output=True,
                timeout=10
            )
            
            # Déplacer dans HDFS
            subprocess.run(
                ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', '-f', '/tmp/seasonal_profile_enriched_temp.json', hdfs_file],
                check=True,
                capture_output=True,
                timeout=10
            )
            
            # Nettoyer
            os.remove(temp_file)
            subprocess.run(
                ['docker', 'exec', 'namenode', 'rm', '-f', '/tmp/seasonal_profile_enriched_temp.json'],
                capture_output=True
            )
            
            status = "✅" if profile_data['validation_status'] == 'complete' else "⚠️"
            print(f"{status} Profil sauvegardé: {hdfs_file} ({len(profile_data['monthly_profiles'])} mois)")
            
        except Exception as e:
            print(f"⚠️  Erreur lors de la sauvegarde pour {city}, {country}, {year}: {e}", file=sys.stderr)


def main():
    """Fonction principale."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Enrichisseur de profils saisonniers - Valide et enrichit avec statistiques de dispersion',
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
    print("📊 Enrichisseur de profils saisonniers")
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
        
        # Extraire le mois
        df_with_month = df.withColumn(
            'month',
            expr("month(to_date(date, 'yyyy-MM-dd'))")
        )
        
        # Valider et enrichir les profils
        enriched_profiles = validate_and_enrich_profiles(df)
        
        if enriched_profiles is None or enriched_profiles.count() == 0:
            print("❌ Aucun profil enrichi calculé", file=sys.stderr)
            sys.exit(1)
        
        # Calculer les quantiles
        final_profiles = calculate_quantiles(df_with_month, enriched_profiles)
        
        # Valider la complétude
        validation_results = validate_profile_completeness(final_profiles)
        
        print(f"\n✅ {final_profiles.select('country', 'city', 'year').distinct().count()} profil(s) ville/année analysé(s)")
        print(f"✅ {final_profiles.count()} profil(s) mensuel(s) enrichi(s)")
        
        # Afficher un aperçu
        print("\n📋 Aperçu des profils enrichis:")
        final_profiles.select(
            'country', 'city', 'year', 'month',
            'avg_temperature', 'std_temperature', 'temp_median',
            'avg_windspeed', 'std_windspeed', 'wind_median'
        ).show(20, truncate=False)
        
        # Sauvegarder dans HDFS
        save_enriched_profiles_to_hdfs(final_profiles, validation_results, args.hdfs_path)
        
        # Résumé de validation
        complete_count = sum(1 for v in validation_results.values() if v['is_complete'])
        incomplete_count = len(validation_results) - complete_count
        
        print("\n" + "=" * 60)
        print("📊 Résumé de validation:")
        print(f"   Profils complets (12 mois): {complete_count}")
        print(f"   Profils incomplets: {incomplete_count}")
        print("=" * 60)
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
