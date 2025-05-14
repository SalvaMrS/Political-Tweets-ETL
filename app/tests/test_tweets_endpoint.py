import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
from main import app
from elasticsearch_service import get_es_client, index_tweet

client = TestClient(app)

# Datos de prueba
TEST_TWEETS = [
    {
        "id": "1",
        "user": {
            "username": "Test User 1",
            "handle": "@test1",
            "verified": True
        },
        "meta": {
            "created_at": "2024-01-15T10:00:00Z",
            "hashtags": ["#test1", "#test2"]
        },
        "payload": {
            "tweet": {
                "content": "This is a test tweet 1"
            }
        },
        "metrics": {
            "likes": 100,
            "retweets": 50,
            "emotion": "joy",
            "stance": "SUPPORT"
        }
    },
    {
        "id": "2",
        "user": {
            "username": "Test User 2",
            "handle": "@test2",
            "verified": False
        },
        "meta": {
            "created_at": "2024-01-16T10:00:00Z",
            "hashtags": ["#test3"]
        },
        "payload": {
            "tweet": {
                "content": "This is a test tweet 2"
            }
        },
        "metrics": {
            "likes": 200,
            "retweets": 100,
            "emotion": "anger",
            "stance": "OPPOSE"
        }
    }
]

@pytest.fixture(autouse=True)
def setup_teardown():
    """Fixture para configurar y limpiar los datos de prueba"""
    es = get_es_client()
    
    # Indexar tweets de prueba
    for tweet in TEST_TWEETS:
        index_tweet(es, tweet)
    
    yield
    
    # Limpiar tweets de prueba
    for tweet in TEST_TWEETS:
        es.delete(index="tweets", id=tweet["id"], ignore=[404])

def test_get_tweets_basic():
    """Test básico para obtener tweets sin filtros"""
    response = client.get("/tweets")
    assert response.status_code == 200
    data = response.json()
    assert "tweets" in data
    assert "total" in data
    assert len(data["tweets"]) > 0

def test_get_tweets_with_date_filter():
    """Test para obtener tweets con filtro de fechas"""
    start_date = "2024-01-15T00:00:00Z"
    end_date = "2024-01-15T23:59:59Z"
    
    response = client.get(f"/tweets?start_date={start_date}&end_date={end_date}")
    assert response.status_code == 200
    data = response.json()
    
    # Verificar que solo obtenemos tweets del día 15
    for tweet in data["tweets"]:
        tweet_date = datetime.fromisoformat(tweet["created_at"].replace('Z', '+00:00'))
        assert tweet_date.date() == datetime.fromisoformat(start_date.replace('Z', '+00:00')).date()

def test_get_tweets_pagination():
    """Test para verificar la paginación"""
    # Primera página
    response1 = client.get("/tweets?limit=1&offset=0")
    assert response1.status_code == 200
    data1 = response1.json()
    assert len(data1["tweets"]) == 1
    
    # Segunda página
    response2 = client.get("/tweets?limit=1&offset=1")
    assert response2.status_code == 200
    data2 = response2.json()
    assert len(data2["tweets"]) == 1
    
    # Verificar que son tweets diferentes
    assert data1["tweets"][0]["id"] != data2["tweets"][0]["id"]

def test_get_tweets_invalid_date_format():
    """Test para verificar el manejo de fechas inválidas"""
    response = client.get("/tweets?start_date=invalid-date")
    assert response.status_code == 400
    assert "Formato de fecha inicial inválido" in response.json()["detail"]

def test_get_tweets_invalid_limit():
    """Test para verificar el manejo de límites inválidos"""
    response = client.get("/tweets?limit=0")
    assert response.status_code == 422  # FastAPI validation error
    
    response = client.get("/tweets?limit=101")
    assert response.status_code == 422

def test_get_tweets_invalid_offset():
    """Test para verificar el manejo de offset inválido"""
    response = client.get("/tweets?offset=-1")
    assert response.status_code == 422

def test_get_tweets_response_structure():
    """Test para verificar la estructura de la respuesta"""
    response = client.get("/tweets")
    assert response.status_code == 200
    data = response.json()
    
    # Verificar estructura del primer tweet
    tweet = data["tweets"][0]
    assert "id" in tweet
    assert "user" in tweet
    assert "content" in tweet
    assert "created_at" in tweet
    assert "hashtags" in tweet
    assert "metrics" in tweet
    
    # Verificar estructura de user
    assert "username" in tweet["user"]
    assert "handle" in tweet["user"]
    assert "verified" in tweet["user"]
    
    # Verificar estructura de metrics
    assert "likes" in tweet["metrics"]
    assert "retweets" in tweet["metrics"]
    
    # Verificar campos opcionales
    assert "emotion" in tweet
    assert "stance" in tweet 