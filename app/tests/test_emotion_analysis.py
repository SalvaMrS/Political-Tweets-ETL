from elasticsearch import Elasticsearch
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from elasticsearch_service import get_es_client, INDEX_NAME
from transformers import pipeline
import json
from pprint import pprint
from datetime import datetime

def setup_emotion_classifier():
    """Configura el clasificador de emociones."""
    try:
        classifier = pipeline(
            "text-classification",
            model="j-hartmann/emotion-english-distilroberta-base",
            return_all_scores=True
        )
        print("✅ Clasificador de emociones configurado correctamente")
        return classifier
    except Exception as e:
        print(f"❌ Error al configurar el clasificador: {str(e)}")
        raise

def analyze_tweets_emotions():
    """Analiza las emociones de los tweets y guarda los resultados en un archivo."""
    try:
        # Configurar cliente de Elasticsearch
        es = get_es_client()
        
        # Configurar clasificador de emociones
        classifier = setup_emotion_classifier()
        
        # Obtener el total de tweets
        count_result = es.count(index=INDEX_NAME)
        total_tweets = count_result['count']
        
        # Buscar todos los tweets
        search_result = es.search(
            index=INDEX_NAME,
            body={
                "query": {
                    "match_all": {}
                },
                "size": total_tweets  # Obtener todos los tweets
            }
        )
        
        print(f"\n📊 Analizando emociones de {total_tweets} tweets...")
        
        # Lista para almacenar los resultados
        results = []
        
        # Procesar cada tweet
        for hit in search_result['hits']['hits']:
            tweet = hit['_source']
            tweet_id = tweet['id']
            content = tweet['payload']['tweet']['content']
            
            # Obtener predicción de emociones
            emotions = classifier(content)[0]
            
            # Encontrar la emoción con mayor puntuación
            top_emotion = max(emotions, key=lambda x: x['score'])
            
            # Crear resultado para este tweet
            result = {
                'tweet_id': tweet_id,
                'content': content,
                'user': tweet['user'],
                'meta': tweet['meta'],
                'metrics': tweet['metrics'],
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
            
            # Mostrar progreso
            print(f"Procesado tweet {tweet_id}/{total_tweets}")
        
        # Crear directorio results si no existe
        results_dir = os.path.join( 'tests', 'results')
        os.makedirs(results_dir, exist_ok=True)
        
        # Generar nombre de archivo con timestamp y referencia al test
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(results_dir, f"test_emotion_analysis_{timestamp}.json")
        
        # Guardar resultados en archivo JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n✅ Análisis completado y guardado en {output_file}")
        print(f"📝 Total de tweets analizados: {len(results)}")
        
    except Exception as e:
        print(f"❌ Error durante el análisis: {str(e)}")
        raise

if __name__ == "__main__":
    analyze_tweets_emotions() 