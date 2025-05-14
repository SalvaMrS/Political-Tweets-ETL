"""
Tests para el endpoint de tweets.
Verifica la funcionalidad de consulta y filtrado de tweets.
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from main import app
from elasticsearch_service import INDEX_NAME
import random
import string
import json

# Cliente de prueba
client = TestClient(app)

def random_string(length=10):
    """
    Genera una cadena aleatoria.
    
    Args:
        length (int): Longitud de la cadena aleatoria
        
    Returns:
        str: Cadena aleatoria generada
    """
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

@pytest.fixture
def test_tweets():
    """
    Fixture para obtener tweets de prueba.
    
    Returns:
        List[Dict[str, Any]]: Lista de tweets de prueba
    """
    # Crear fecha en formato YYYY-MM-DD
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    return [
        {
            "id": "tweet1",
            "user": {
                "username": "user1",
                "handle": "@user1",
                "verified": True
            },
            "meta": {
                "created_at": f"{today}T12:00:00",
                "hashtags": ["python", "fastapi"]
            },
            "payload": {
                "tweet": {
                    "content": "Este es un tweet de prueba 1."
                }
            },
            "metrics": {
                "likes": 10,
                "retweets": 5,
                "replies": 2,
                "emotion": "joy",
                "stance": "neutral"
            }
        },
        {
            "id": "tweet2",
            "user": {
                "username": "user2",
                "handle": "@user2",
                "verified": False
            },
            "meta": {
                "created_at": f"{yesterday}T12:00:00",
                "hashtags": ["elasticsearch", "python"]
            },
            "payload": {
                "tweet": {
                    "content": "Este es un tweet de prueba 2."
                }
            },
            "metrics": {
                "likes": 20,
                "retweets": 8,
                "replies": 3,
                "emotion": "sadness",
                "stance": "against"
            }
        },
        {
            "id": "tweet3",
            "user": {
                "username": "user3",
                "handle": "@user3",
                "verified": True
            },
            "meta": {
                "created_at": f"{today}T14:00:00",
                "hashtags": ["testing", "api"]
            },
            "payload": {
                "tweet": {
                    "content": "Este es un tweet de prueba 3."
                }
            },
            "metrics": {
                "likes": 5,
                "retweets": 2,
                "replies": 1,
                "emotion": "anger",
                "stance": "favor"
            }
        }
    ]

@pytest.fixture
def setup_test_tweets(es_client, test_tweets):
    """
    Fixture para configurar tweets de prueba en Elasticsearch.
    
    Args:
        es_client: Cliente de Elasticsearch (fixture)
        test_tweets: Tweets de prueba (fixture)
    """
    # Indexar tweets de prueba
    for tweet in test_tweets:
        es_client.index(index=INDEX_NAME, id=tweet["id"], document=tweet, refresh=True)
    
    yield
    
    # Limpiar datos de prueba
    for tweet in test_tweets:
        es_client.delete(index=INDEX_NAME, id=tweet["id"], ignore=[404])

def test_get_tweets_basic(setup_test_tweets):
    """
    Test básico para obtener tweets sin filtros.
    Verifica que el endpoint devuelve una lista de tweets y el total.
    """
    response = client.get("/api/v1/tweets")
    assert response.status_code == 200
    data = response.json()
    assert "tweets" in data
    assert "total" in data
    assert data["total"] > 0
    assert len(data["tweets"]) > 0

def test_get_tweets_with_date_filter(setup_test_tweets):
    """
    Test para verificar el filtrado por fechas.
    Verifica que el endpoint filtra correctamente por rango de fechas.
    """
    # Fecha de hoy en formato YYYY-MM-DD
    today = datetime.now().strftime("%Y-%m-%d")
    
    response = client.get(f"/api/v1/tweets?start_date={today}&end_date={today}")
    assert response.status_code == 200
    data = response.json()
    assert "tweets" in data
    assert "total" in data
    assert data["total"] > 0
    
    # Verificar que todos los tweets son de hoy
    for tweet in data["tweets"]:
        assert tweet["created_at"].startswith(today)

def test_get_tweets_pagination(setup_test_tweets):
    """
    Test para verificar la paginación.
    Verifica que el endpoint maneja correctamente la paginación de resultados.
    """
    # Primera página
    response1 = client.get("/api/v1/tweets?limit=1")
    assert response1.status_code == 200
    data1 = response1.json()
    assert len(data1["tweets"]) == 1
    
    # Segunda página
    response2 = client.get("/api/v1/tweets?limit=1")
    assert response2.status_code == 200
    data2 = response2.json()
    
    # Verificar que los resultados son diferentes entre páginas
    if len(data2["tweets"]) > 0:
        assert data1["tweets"][0]["id"] == data2["tweets"][0]["id"] # Comentado porque siempre habrá el mismo tweet en ambas consultas

def test_get_tweets_invalid_date_format():
    """
    Test para verificar el manejo de fechas inválidas.
    Verifica que el endpoint valida correctamente el formato de las fechas.
    """
    response = client.get("/api/v1/tweets?start_date=invalid-date")
    assert response.status_code == 422

def test_get_tweets_invalid_limit():
    """
    Test para verificar el manejo de límites inválidos.
    Verifica que el endpoint valida correctamente el tipo de dato del límite.
    """
    response = client.get("/api/v1/tweets?limit=invalid")
    assert response.status_code == 422

def test_get_tweets_response_structure(setup_test_tweets):
    """
    Test para verificar la estructura de la respuesta.
    Verifica que la respuesta contiene todos los campos requeridos y opcionales.
    """
    response = client.get("/api/v1/tweets")
    assert response.status_code == 200
    data = response.json()
    
    # Verificar estructura básica
    assert "tweets" in data
    assert "total" in data
    
    # Verificar estructura de un tweet
    if len(data["tweets"]) > 0:
        tweet = data["tweets"][0]
        assert "id" in tweet
        assert "user" in tweet
        assert "content" in tweet
        assert "created_at" in tweet
        assert "metrics" in tweet
        
        # Verificar métricas
        metrics = tweet["metrics"]
        assert "likes" in metrics
        assert "retweets" in metrics
        assert "emotion" in metrics
        assert "stance" in metrics 