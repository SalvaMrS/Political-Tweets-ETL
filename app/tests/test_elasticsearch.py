from elasticsearch import Elasticsearch
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from elasticsearch_service import get_es_client, INDEX_NAME, ES_MAPPING
import json
from pprint import pprint

def verify_mapping_fields():
    """Verifica que todos los campos del mapeo estén presentes en el índice."""
    es = get_es_client()
    index_mapping = es.indices.get_mapping(index=INDEX_NAME)
    index_properties = index_mapping[INDEX_NAME]['mappings']['properties']
    
    print("\n🔍 Verificación de campos del mapeo:")
    for field, config in ES_MAPPING['mappings']['properties'].items():
        if field in index_properties:
            print(f"✅ Campo '{field}' presente")
            if 'properties' in config:
                for subfield in config['properties']:
                    if subfield in index_properties[field]['properties']:
                        print(f"  ✅ Subcampo '{field}.{subfield}' presente")
                    else:
                        print(f"  ❌ Subcampo '{field}.{subfield}' NO presente")
        else:
            print(f"❌ Campo '{field}' NO presente")

def analyze_first_tweet():
    """Analiza en detalle el primer tweet del índice."""
    es = get_es_client()
    result = es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "match_all": {}
            },
            "size": 1
        }
    )
    
    if result['hits']['hits']:
        tweet = result['hits']['hits'][0]['_source']
        print("\n📝 Análisis detallado del primer tweet:")
        
        # Verificar campos requeridos
        required_fields = {
            'id': 'ID del tweet',
            'user.username': 'Nombre de usuario',
            'user.handle': 'Handle del usuario',
            'user.verified': 'Estado de verificación',
            'meta.created_at': 'Fecha de creación',
            'meta.hashtags': 'Hashtags',
            'payload.tweet.content': 'Contenido del tweet',
            'metrics.retweets': 'Número de retweets',
            'metrics.likes': 'Número de likes'
        }
        
        # Verificar campos opcionales (None por defecto)
        optional_fields = {
            'metrics.emotion': 'Emoción (None por defecto)',
            'metrics.stance': 'Postura (None por defecto)'
        }
        
        # Verificar campos requeridos
        for field, description in required_fields.items():
            value = tweet
            for key in field.split('.'):
                value = value.get(key, None)
            status = "✅" if value is not None else "❌"
            print(f"{status} {description}: {value}")
        
        # Verificar campos opcionales
        for field, description in optional_fields.items():
            value = tweet
            for key in field.split('.'):
                value = value.get(key, None)
            # Para campos opcionales, None es un valor válido
            print(f"✅ {description}: {value}")

def test_elasticsearch_connection():
    """Prueba la conexión con Elasticsearch y muestra información del índice."""
    try:
        # Obtener cliente de Elasticsearch
        es = get_es_client()
        
        # Verificar si el índice existe
        if es.indices.exists(index=INDEX_NAME):
            print(f"\n✅ Índice '{INDEX_NAME}' existe")
            
            # Verificar campos del mapeo
            verify_mapping_fields()
            
            # Analizar el primer tweet
            analyze_first_tweet()
            
            # Obtener estadísticas del índice
            stats = es.indices.stats(index=INDEX_NAME)
            print(f"\n📈 Total documentos en el índice: {stats['indices'][INDEX_NAME]['total']['docs']['count']}")
            
        else:
            print(f"❌ El índice '{INDEX_NAME}' no existe")
            
    except Exception as e:
        print(f"❌ Error al conectar con Elasticsearch: {str(e)}")

if __name__ == "__main__":
    test_elasticsearch_connection() 