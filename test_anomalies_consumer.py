#!/usr/bin/env python3
"""
Script de test pour consommer les anomalies depuis Kafka
"""

import sys
import json
from kafka import KafkaConsumer
from datetime import datetime


def consume_anomalies(topic='weather_anomalies', kafka_servers='localhost:9092'):
    """Consomme les anomalies depuis Kafka et les affiche."""
    
    print("=" * 60)
    print("🔍 Consommateur d'anomalies climatiques")
    print("=" * 60)
    print(f"📡 Topic: {topic}")
    print(f"🌐 Serveurs Kafka: {kafka_servers}")
    print("=" * 60)
    print()
    print("⏳ En attente d'anomalies...")
    print("   (Appuyez sur Ctrl+C pour arrêter)")
    print()
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers.split(','),
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            consumer_timeout_ms=10000  # Timeout de 10 secondes
        )
        
        anomaly_count = 0
        
        for message in consumer:
            anomaly_count += 1
            anomaly = message.value
            
            print(f"\n🚨 Anomalie #{anomaly_count}")
            print("-" * 60)
            print(f"📍 Localisation: {anomaly.get('city', 'N/A')}, {anomaly.get('country', 'N/A')}")
            print(f"🕐 Temps: {anomaly.get('event_time', 'N/A')}")
            print(f"📊 Variable: {anomaly.get('variable', 'N/A')}")
            print(f"🔴 Type: {anomaly.get('anomaly_type', 'N/A')}")
            print(f"📈 Observé: {anomaly.get('observed_value', 'N/A')}")
            print(f"📉 Attendu: {anomaly.get('expected_value', 'N/A')}")
            
            # Calculer l'écart si possible
            observed = anomaly.get('observed_value')
            expected = anomaly.get('expected_value')
            if observed is not None and expected is not None:
                deviation = observed - expected
                print(f"📊 Écart: {deviation:+.2f}")
            
            print("-" * 60)
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt du consommateur...")
        print(f"✅ {anomaly_count} anomalie(s) reçue(s)")
    except Exception as e:
        print(f"❌ Erreur: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'consumer' in locals():
            consumer.close()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Consommer les anomalies depuis Kafka',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'topic',
        nargs='?',
        default='weather_anomalies',
        help='Topic Kafka à consommer (défaut: weather_anomalies)'
    )
    
    parser.add_argument(
        '--kafka-servers',
        default='localhost:9092',
        help='Serveurs Kafka bootstrap (défaut: localhost:9092)'
    )
    
    args = parser.parse_args()
    
    consume_anomalies(args.topic, args.kafka_servers)
