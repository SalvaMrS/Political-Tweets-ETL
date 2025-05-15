"""
Tests para el endpoint de análisis de emociones.
Verifica la funcionalidad de análisis de emociones en tweets.
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from main import app
from elasticsearch_service import INDEX_NAME
import json
from pathlib import Path

# Cliente de prueba
client = TestClient(app)

@pytest.fixture
def sample_tweet(es_client):
    """
    Fixture para obtener una muestra de tweet para pruebas.
    
    Args:
        es_client: Cliente de Elasticsearch (fixture)
    
    Returns:
        Dict[str, Any]: Tweet de prueba
    """
    # Crear una fecha en el formato correcto YYYY-MM-DD
    today = datetime.now().strftime("%Y-%m-%d")
    
    tweet = {
        "id": "test_tweet_id",
        "user": {
            "username": "test_user",
            "handle": "@test",
            "verified": True
        },
        "meta": {
            "created_at": f"{today}T12:00:00",
            "hashtags": ["test"]
        },
        "payload": {
            "tweet": {
                "content": "I am feeling very happy today! This is amazing!"
            }
        },
        "metrics": {
            "retweets": 0,
            "likes": 0,
            "emotion": None,
            "stance": None
        }
    }
    
    # Indexar el tweet
    es_client.index(index=INDEX_NAME, document=tweet, refresh=True)
    return tweet

@pytest.fixture(autouse=True)
def setup_teardown(es_client, sample_tweet):
    """
    Fixture para configurar y limpiar el entorno de pruebas.
    
    Args:
        es_client: Cliente de Elasticsearch (fixture)
        sample_tweet: Tweet de prueba (fixture)
    """
    yield
    
    # Limpiar datos de prueba
    es_client.delete(index=INDEX_NAME, id=sample_tweet["id"], ignore=[404])

def test_emotion_endpoint_success(sample_tweet):
    """
    Prueba el caso exitoso del endpoint de emociones.
    Verifica que el endpoint procese correctamente los tweets y devuelva un mensaje de éxito.
    
    Args:
        sample_tweet: Tweet de prueba (fixture)
    """
    response = client.get("/emotion")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "processed" in data
    assert data["processed"] > 0

def test_emotion_endpoint_with_dates(sample_tweet):
    """
    Prueba el endpoint con filtros de fecha.
    Verifica que el endpoint funcione correctamente con rangos de fechas.
    """
    # Obtenemos la fecha de hoy en formato YYYY-MM-DD
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    
    # Consulta con fecha de hoy
    response = client.get(f"/emotion?start_date={yesterday}&end_date={tomorrow}")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "processed" in data
    assert data["processed"] > 0  # Debería procesar al menos el tweet de muestra

def test_emotion_endpoint_with_limit(sample_tweet):
    """
    Prueba el endpoint con límite de tweets.
    Verifica que el endpoint respete el límite especificado.
    
    Args:
        sample_tweet: Tweet de prueba (fixture)
    """
    response = client.get("/emotion?limit=1")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "processed" in data
    assert data["processed"] <= 1

def test_emotion_endpoint_no_tweets():
    """
    Prueba el caso cuando no hay tweets en el rango especificado.
    Verifica que el endpoint maneje correctamente la ausencia de tweets.
    """
    # Usamos una fecha muy futura para asegurar que no haya tweets
    future_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
    
    response = client.get(f"/emotion?start_date={future_date}&end_date={future_date}")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "processed" in data
    assert data["processed"] == 0

def test_emotion_endpoint_invalid_dates():
    """
    Prueba el endpoint con fechas inválidas.
    Verifica que el endpoint valide correctamente el formato de las fechas.
    """
    response = client.get("/emotion?start_date=invalid-date")
    assert response.status_code == 422

def test_emotion_endpoint_invalid_limit():
    """
    Prueba el endpoint con un límite inválido.
    Verifica que el endpoint valide correctamente el tipo de dato del límite.
    """
    response = client.get("/emotion?limit=not-a-number")
    assert response.status_code == 422

def test_emotion_analysis_results(es_client, sample_tweet):
    """
    Prueba que el análisis de emociones se guarda correctamente en Elasticsearch.
    Verifica la estructura y validez de los resultados del análisis.
    
    Args:
        es_client: Cliente de Elasticsearch (fixture)
        sample_tweet: Tweet de prueba (fixture)
    """
    # Asegurar que el tweet está indexado antes de la prueba
    es_client.index(index=INDEX_NAME, id=sample_tweet["id"], document=sample_tweet, refresh=True)
    
    # Ejecutar análisis sin filtros para incluir el tweet de prueba
    response = client.get("/emotion")
    assert response.status_code == 200
    
    # Verificar que el tweet fue actualizado con la emoción
    result = es_client.get(index=INDEX_NAME, id=sample_tweet["id"])
    assert "metrics" in result["_source"]
    assert "emotion" in result["_source"]["metrics"]
    assert result["_source"]["metrics"]["emotion"] is not None 