#!/usr/bin/env python3
"""
Script consommateur Kafka - Exercice 2
Lit les messages depuis un topic Kafka et les affiche en temps réel.
"""

import argparse
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json


def create_consumer(topic, bootstrap_servers='localhost:29092', group_id=None, from_beginning=False):
    """
    Crée et configure un consommateur Kafka.
    
    Args:
        topic: Nom du topic Kafka à consommer
        bootstrap_servers: Adresse du serveur Kafka (par défaut: localhost:29092)
        group_id: ID du groupe de consommateurs (None pour un consommateur unique)
        from_beginning: Si True, lit depuis le début du topic
    
    Returns:
        KafkaConsumer configuré
    """
    consumer_config = {
        'bootstrap_servers': bootstrap_servers,
        'auto_offset_reset': 'earliest' if from_beginning else 'latest',
        'enable_auto_commit': True,
        'value_deserializer': lambda x: x.decode('utf-8'),
        # consumer_timeout_ms n'est pas défini pour attendre indéfiniment (comportement par défaut)
    }
    
    if group_id:
        consumer_config['group_id'] = group_id
    
    try:
        consumer = KafkaConsumer(topic, **consumer_config)
        return consumer
    except KafkaError as e:
        print(f"Erreur lors de la création du consommateur: {e}", file=sys.stderr)
        sys.exit(1)


def consume_messages(consumer, topic):
    """
    Consomme et affiche les messages du topic en temps réel.
    
    Args:
        consumer: Instance KafkaConsumer
        topic: Nom du topic (pour affichage)
    """
    print(f"📡 Connexion au topic '{topic}'...")
    print(f"✅ Connecté! En attente de messages... (Ctrl+C pour arrêter)\n")
    print("-" * 60)
    
    try:
        message_count = 0
        for message in consumer:
            message_count += 1
            # Afficher les informations du message
            print(f"\n📨 Message reçu:")
            print(f"   Topic: {message.topic}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            if hasattr(message, 'timestamp') and message.timestamp:
                print(f"   Timestamp: {message.timestamp}")
            
            # Essayer de parser le message comme JSON, sinon afficher tel quel
            try:
                message_value = json.loads(message.value)
                print(f"   Contenu (JSON):")
                print(f"   {json.dumps(message_value, indent=6, ensure_ascii=False)}")
            except json.JSONDecodeError:
                print(f"   Contenu (texte):")
                print(f"   {message.value}")
            
            print("-" * 60)
        
        # Si on sort de la boucle sans messages et qu'on attend depuis le début
        if message_count == 0:
            print("\n⚠️  Aucun message trouvé dans le topic.")
            print("   Le consommateur attend de nouveaux messages...")
            print("   (Envoyez un message dans un autre terminal pour le voir apparaître)")
            print("   (Appuyez sur Ctrl+C pour arrêter)")
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Arrêt du consommateur...")
    except KafkaError as e:
        print(f"\n❌ Erreur Kafka: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erreur inattendue: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()
        print("✅ Consommateur fermé.")


def main():
    """Fonction principale."""
    parser = argparse.ArgumentParser(
        description='Consommateur Kafka - Lit les messages depuis un topic et les affiche en temps réel',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  %(prog)s weather_stream
  %(prog)s weather_stream --from-beginning
  %(prog)s weather_stream --bootstrap-servers localhost:29092 --group-id my-group
        """
    )
    
    parser.add_argument(
        'topic',
        help='Nom du topic Kafka à consommer'
    )
    
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:29092',
        help='Adresse du serveur Kafka (défaut: localhost:29092)'
    )
    
    parser.add_argument(
        '--group-id',
        default=None,
        help='ID du groupe de consommateurs (optionnel)'
    )
    
    parser.add_argument(
        '--from-beginning',
        action='store_true',
        help='Lire les messages depuis le début du topic'
    )
    
    args = parser.parse_args()
    
    # Créer le consommateur
    consumer = create_consumer(
        topic=args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        from_beginning=args.from_beginning
    )
    
    # Consommer les messages
    consume_messages(consumer, args.topic)


if __name__ == '__main__':
    main()
