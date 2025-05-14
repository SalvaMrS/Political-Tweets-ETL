import pytest
from elasticsearch import Elasticsearch
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from elasticsearch_service import get_es_client, INDEX_NAME, ES_MAPPING
import json
from pprint import pprint

@pytest.fixture
def es_client():
    """Fixture para obtener el cliente de Elasticsearch."""
    return get_es_client()

def test_elasticsearch_connection(es_client):
    """Prueba la conexión con Elasticsearch."""
    assert es_client.ping(), "No se pudo conectar con Elasticsearch"

def test_index_exists(es_client):
    """Prueba que el índice existe."""
    assert es_client.indices.exists(index=INDEX_NAME), f"El índice '{INDEX_NAME}' no existe"

def test_mapping_fields(es_client):
    """Verifica que todos los campos del mapeo estén presentes en el índice."""
    index_mapping = es_client.indices.get_mapping(index=INDEX_NAME)
    index_properties = index_mapping[INDEX_NAME]['mappings']['properties']
    
    # Verificar campos principales
    for field, config in ES_MAPPING['mappings']['properties'].items():
        assert field in index_properties, f"Campo '{field}' no presente en el índice"
        
        # Verificar subcampos si existen
        if 'properties' in config:
            for subfield in config['properties']:
                assert subfield in index_properties[field]['properties'], \
                    f"Subcampo '{field}.{subfield}' no presente en el índice"

def test_first_tweet_structure(es_client):
    """Analiza la estructura del primer tweet del índice."""
    result = es_client.search(
        index=INDEX_NAME,
        body={
            "query": {
                "match_all": {}
            },
            "size": 1
        }
    )
    
    assert result['hits']['hits'], "No se encontraron tweets en el índice"
    
    tweet = result['hits']['hits'][0]['_source']
    
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
    
    for field, description in required_fields.items():
        value = tweet
        for key in field.split('.'):
            value = value.get(key, None)
        assert value is not None, f"Campo requerido '{description}' no presente o es None"

def test_index_stats(es_client):
    """Verifica las estadísticas básicas del índice."""
    stats = es_client.indices.stats(index=INDEX_NAME)
    assert stats['indices'][INDEX_NAME]['total']['docs']['count'] > 0, \
        "El índice no contiene documentos"

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
            test_mapping_fields(es)
            
            # Analizar el primer tweet
            test_first_tweet_structure(es)
            
            # Obtener estadísticas del índice
            test_index_stats(es)
            
        else:
            print(f"❌ El índice '{INDEX_NAME}' no existe")
            
    except Exception as e:
        print(f"❌ Error al conectar con Elasticsearch: {str(e)}")

if __name__ == "__main__":
    test_elasticsearch_connection() 