import pytest
import os
import json
from datetime import datetime
from elasticsearch_service import get_es_client, INDEX_NAME
from transformers import pipeline

@pytest.fixture
def es_client():
    """Fixture para obtener el cliente de Elasticsearch."""
    return get_es_client()

@pytest.fixture
def emotion_classifier():
    """Fixture para el clasificador de emociones."""
    return pipeline(
        "text-classification",
        model="j-hartmann/emotion-english-distilroberta-base",
        return_all_scores=True
    )

@pytest.fixture
def sample_tweets(es_client):
    """Fixture para obtener una muestra de tweets para pruebas."""
    result = es_client.search(
        index=INDEX_NAME,
        body={
            "query": {
                "match_all": {}
            },
            "size": 5  # Limitamos a 5 tweets para pruebas
        }
    )
    return result['hits']['hits']

def test_emotion_classifier_setup(emotion_classifier):
    """Prueba la configuración del clasificador de emociones."""
    assert emotion_classifier is not None, "El clasificador de emociones no se configuró correctamente"

def test_emotion_analysis_on_tweets(emotion_classifier, sample_tweets):
    """Prueba el análisis de emociones en una muestra de tweets."""
    assert sample_tweets, "No se encontraron tweets para analizar"
    
    for hit in sample_tweets:
        tweet = hit['_source']
        content = tweet['payload']['tweet']['content']
        
        # Obtener predicción de emociones
        emotions = emotion_classifier(content)[0]
        
        # Verificar estructura de la respuesta
        assert isinstance(emotions, list), "La respuesta del clasificador debe ser una lista"
        assert len(emotions) > 0, "La respuesta del clasificador debe contener al menos una emoción"
        
        # Verificar estructura de cada emoción
        for emotion in emotions:
            assert 'label' in emotion, "Cada emoción debe tener un label"
            assert 'score' in emotion, "Cada emoción debe tener un score"
            assert isinstance(emotion['score'], float), "El score debe ser un float"
            assert 0 <= emotion['score'] <= 1, "El score debe estar entre 0 y 1"

def test_emotion_analysis_results_structure(emotion_classifier, sample_tweets):
    """Prueba la estructura de los resultados del análisis de emociones."""
    results = []
    
    for hit in sample_tweets:
        tweet = hit['_source']
        content = tweet['payload']['tweet']['content']
        emotions = emotion_classifier(content)[0]
        
        # Encontrar la emoción dominante
        top_emotion = max(emotions, key=lambda x: x['score'])
        
        result = {
            'tweet_id': tweet['id'],
            'content': content,
            'emotion_analysis': {
                'dominant_emotion': {
                    'label': top_emotion['label'],
                    'score': float(top_emotion['score'])
                },
                'all_emotions': [
                    {
                        'label': emotion['label'],
                        'score': float(emotion['score'])
                    }
                    for emotion in sorted(emotions, key=lambda x: x['score'], reverse=True)
                ]
            }
        }
        
        results.append(result)
    
    # Verificar estructura de los resultados
    for result in results:
        assert 'tweet_id' in result, "Cada resultado debe tener un tweet_id"
        assert 'content' in result, "Cada resultado debe tener un content"
        assert 'emotion_analysis' in result, "Cada resultado debe tener emotion_analysis"
        
        emotion_analysis = result['emotion_analysis']
        assert 'dominant_emotion' in emotion_analysis, "Debe haber una emoción dominante"
        assert 'all_emotions' in emotion_analysis, "Debe haber una lista de todas las emociones"
        
        dominant = emotion_analysis['dominant_emotion']
        assert 'label' in dominant, "La emoción dominante debe tener un label"
        assert 'score' in dominant, "La emoción dominante debe tener un score"
        
        all_emotions = emotion_analysis['all_emotions']
        assert isinstance(all_emotions, list), "all_emotions debe ser una lista"
        assert len(all_emotions) > 0, "Debe haber al menos una emoción en all_emotions"

def test_emotion_analysis_results_persistence(emotion_classifier, sample_tweets):
    """Prueba la persistencia de los resultados del análisis."""
    results = []
    
    for hit in sample_tweets:
        tweet = hit['_source']
        content = tweet['payload']['tweet']['content']
        emotions = emotion_classifier(content)[0]
        top_emotion = max(emotions, key=lambda x: x['score'])
        
        result = {
            'tweet_id': tweet['id'],
            'content': content,
            'emotion_analysis': {
                'dominant_emotion': {
                    'label': top_emotion['label'],
                    'score': float(top_emotion['score'])
                },
                'all_emotions': [
                    {
                        'label': emotion['label'],
                        'score': float(emotion['score'])
                    }
                    for emotion in sorted(emotions, key=lambda x: x['score'], reverse=True)
                ]
            }
        }
        results.append(result)
    
    # Crear directorio results si no existe
    results_dir = os.path.join('tests', 'results')
    os.makedirs(results_dir, exist_ok=True)
    
    # Generar nombre de archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(results_dir, f"test_emotion_analysis_{timestamp}.json")
    
    # Guardar resultados
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    # Verificar que el archivo se creó correctamente
    assert os.path.exists(output_file), "El archivo de resultados no se creó"
    
    # Verificar que el archivo contiene datos válidos
    with open(output_file, 'r', encoding='utf-8') as f:
        saved_results = json.load(f)
        assert isinstance(saved_results, list), "Los resultados guardados deben ser una lista"
        assert len(saved_results) == len(results), "La cantidad de resultados guardados no coincide" 