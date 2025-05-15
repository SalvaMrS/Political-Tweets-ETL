"""
Tests para el análisis de emociones en tweets.
Verifica el funcionamiento del clasificador de emociones y la estructura de sus resultados.
"""

import pytest
import os
import json
import time
from datetime import datetime
from elasticsearch_service import get_es_client, INDEX_NAME
from transformers import pipeline
from typing import Dict, Any, List

# Constantes para pruebas
RESULTS_DIR = os.path.join('tests', 'results')

@pytest.fixture
def es_client():
    """
    Fixture para obtener el cliente de Elasticsearch.
    
    Returns:
        Elasticsearch: Cliente configurado para las pruebas
    """
    return get_es_client()

@pytest.fixture
def emotion_classifier():
    """
    Fixture para el clasificador de emociones.
    Inicializa el modelo de clasificación de emociones.
    
    Returns:
        Pipeline: Clasificador de emociones configurado
    """
    return pipeline(
        "text-classification",
        model="j-hartmann/emotion-english-distilroberta-base",
        return_all_scores=True
    )

@pytest.fixture
def all_tweets(es_client) -> List[Dict[str, Any]]:
    """
    Fixture para obtener todos los tweets para pruebas.
    
    Args:
        es_client: Cliente de Elasticsearch (fixture)
        
    Returns:
        List[Dict[str, Any]]: Lista de tweets para pruebas
    """
    result = es_client.search(
        index=INDEX_NAME,
        body={
            "query": {"match_all": {}},
            "size": 10000  # Aumentar el tamaño para obtener más tweets
        }
    )
    return result['hits']['hits']

def test_emotion_classifier_setup(emotion_classifier):
    """
    Prueba la configuración del clasificador de emociones.
    Verifica que el clasificador se haya inicializado correctamente.
    
    Args:
        emotion_classifier: Clasificador de emociones (fixture)
    """
    assert emotion_classifier is not None, "El clasificador de emociones no se configuró correctamente"

def test_emotion_analysis_on_tweets(emotion_classifier, all_tweets):
    """
    Prueba el análisis de emociones en todos los tweets.
    Verifica que el clasificador procese correctamente cada tweet y devuelva resultados válidos.
    
    Args:
        emotion_classifier: Clasificador de emociones (fixture)
        all_tweets: Lista de tweets para analizar (fixture)
    """
    assert all_tweets, "No se encontraron tweets para analizar"
    
    results = []
    total_time = 0
    
    for hit in all_tweets:
        tweet = hit['_source']
        content = tweet['payload']['tweet']['content']
        
        # Medir tiempo de inferencia
        start_time = time.time()
        emotions = emotion_classifier(content)[0]
        inference_time = time.time() - start_time
        total_time += inference_time
        
        # Verificar estructura de la respuesta
        assert isinstance(emotions, list), "La respuesta del clasificador debe ser una lista"
        assert len(emotions) > 0, "La respuesta del clasificador debe contener al menos una emoción"
        
        # Verificar estructura de cada emoción
        for emotion in emotions:
            assert 'label' in emotion, "Cada emoción debe tener un label"
            assert 'score' in emotion, "Cada emoción debe tener un score"
            assert isinstance(emotion['score'], float), "El score debe ser un float"
            assert 0 <= emotion['score'] <= 1, "El score debe estar entre 0 y 1"
        
        # Encontrar la emoción dominante
        top_emotion = max(emotions, key=lambda x: x['score'])
        
        result = {
            'tweet_id': tweet['id'],
            'content': content,
            'inference_time': inference_time,
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
    
    # Calcular estadísticas
    avg_time = total_time / len(all_tweets)
    print(f"\nEstadísticas de procesamiento:")
    print(f"Total de tweets procesados: {len(all_tweets)}")
    print(f"Tiempo total de procesamiento: {total_time:.2f} segundos")
    print(f"Tiempo promedio por tweet: {avg_time:.2f} segundos")
    
    # Crear directorio results si no existe
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    # Generar nombre de archivo con timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(RESULTS_DIR, f"test_emotion_analysis_{timestamp}.json")
    
    # Guardar resultados
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\nResultados guardados en: {output_file}")
    
    # Verificar que el archivo se creó correctamente
    assert os.path.exists(output_file), "El archivo de resultados no se creó"
    
    # Verificar que el archivo contiene datos válidos
    with open(output_file, 'r', encoding='utf-8') as f:
        saved_results = json.load(f)
        assert isinstance(saved_results, list), "Los resultados guardados deben ser una lista"
        assert len(saved_results) == len(results), "La cantidad de resultados guardados no coincide" 