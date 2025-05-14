import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from main import app
from elasticsearch_service import get_es_client, INDEX_NAME
import json
import asyncio
import time

client = TestClient(app)

@pytest.fixture
def es_client():
    """Fixture para obtener el cliente de Elasticsearch."""
    return get_es_client()

@pytest.fixture
def sample_tweet():
    """Fixture que proporciona un tweet de ejemplo."""
    return {
        "id": "test_tweet_id",
        "payload": {
            "tweet": {
                "content": "I am feeling very happy today! This is amazing!"
            }
        },
        "meta": {
            "created_at": datetime.now().isoformat()
        }
    }

@pytest.fixture
def indexed_tweet(es_client, sample_tweet):
    """Fixture que indexa un tweet de prueba y lo limpia después."""
    # Indexar el tweet
    es_client.index(
        index=INDEX_NAME,
        id=sample_tweet["id"],
        document=sample_tweet,  # Usar document en lugar de body
        refresh=True  # Forzar refresh para que esté disponible inmediatamente
    )
    
    yield sample_tweet
    
    # Limpiar después de la prueba
    try:
        es_client.delete(
            index=INDEX_NAME,
            id=sample_tweet["id"],
            refresh=True
        )
    except:
        pass

def test_emotion_endpoint_success(indexed_tweet):
    """Prueba el caso exitoso del endpoint de emociones."""
    response = client.post("/emotion", json={})
    assert response.status_code == 200
    
    data = response.json()
    assert "message" in data
    assert "Clasificación completada con éxito" in data["message"]
    assert "tweets procesados en" in data["message"]

def test_emotion_endpoint_with_dates():
    """Prueba el endpoint con filtros de fecha."""
    start_date = (datetime.now() - timedelta(days=7)).isoformat()
    end_date = datetime.now().isoformat()
    
    response = client.post(
        "/emotion",
        json={
            "start_date": start_date,
            "end_date": end_date
        }
    )
    assert response.status_code == 200
    
    data = response.json()
    assert "message" in data

def test_emotion_endpoint_with_limit(indexed_tweet):
    """Prueba el endpoint con límite de tweets."""
    response = client.post(
        "/emotion",
        json={
            "limit": 1
        }
    )
    assert response.status_code == 200
    
    data = response.json()
    assert "message" in data
    assert "1 tweets procesados" in data["message"]

def test_emotion_endpoint_no_tweets():
    """Prueba el caso cuando no hay tweets en el rango especificado."""
    future_date = (datetime.now() + timedelta(days=365)).isoformat()
    
    response = client.post(
        "/emotion",
        json={
            "start_date": future_date,
            "end_date": future_date
        }
    )
    assert response.status_code == 200
    
    data = response.json()
    assert data["message"] == "No se encontraron tweets en el rango de fechas especificado."

def test_emotion_endpoint_invalid_dates():
    """Prueba el endpoint con fechas inválidas."""
    response = client.post(
        "/emotion",
        json={
            "start_date": "invalid-date",
            "end_date": "2024-03-14"
        }
    )
    assert response.status_code in [400, 422]  # FastAPI validation error

def test_emotion_endpoint_invalid_limit():
    """Prueba el endpoint con un límite inválido."""
    response = client.post(
        "/emotion",
        json={
            "limit": "not-a-number"
        }
    )
    assert response.status_code in [400, 422]  # FastAPI validation error

@pytest.mark.asyncio
async def test_emotion_analysis_results(es_client, indexed_tweet):
    """Prueba que el análisis de emociones se guarda correctamente en Elasticsearch."""
    # Primero ejecutamos el análisis
    response = client.post("/emotion", json={})
    assert response.status_code == 200
    
    # Esperar a que Elasticsearch procese la actualización
    max_retries = 5
    retry_delay = 1
    
    for _ in range(max_retries):
        try:
            # Verificar que el documento se actualizó correctamente
            doc = es_client.get(
                index=INDEX_NAME,
                id=indexed_tweet["id"]
            )
            
            if "emotion_analysis" in doc["_source"]:
                emotion_analysis = doc["_source"]["emotion_analysis"]
                
                # Verificar estructura del análisis de emociones
                assert "dominant_emotion" in emotion_analysis
                assert "all_emotions" in emotion_analysis
                
                dominant = emotion_analysis["dominant_emotion"]
                assert "label" in dominant
                assert "score" in dominant
                assert isinstance(dominant["score"], float)
                assert 0 <= dominant["score"] <= 1
                
                all_emotions = emotion_analysis["all_emotions"]
                assert isinstance(all_emotions, list)
                assert len(all_emotions) > 0
                for emotion in all_emotions:
                    assert "label" in emotion
                    assert "score" in emotion
                    assert isinstance(emotion["score"], float)
                    assert 0 <= emotion["score"] <= 1
                
                # Si llegamos aquí, todas las verificaciones pasaron
                return
                
        except Exception as e:
            print(f"Intento fallido: {str(e)}")
        
        # Esperar antes del siguiente intento
        await asyncio.sleep(retry_delay)
    
    # Si llegamos aquí, todos los intentos fallaron
    assert False, "No se pudo verificar el análisis de emociones después de varios intentos" 