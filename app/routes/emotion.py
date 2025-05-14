from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime, time
import time as time_lib
from transformers import pipeline
from elasticsearch_service import get_es_client, INDEX_NAME
from pydantic import BaseModel
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

# Inicializar el clasificador de emociones
emotion_classifier = pipeline(
    "text-classification",
    model="j-hartmann/emotion-english-distilroberta-base",
    return_all_scores=True
)

class EmotionResponse(BaseModel):
    message: str

@router.post(
    "/emotion",
    response_model=EmotionResponse,
    summary="Analizar emociones en tweets",
    description="""
    Analiza las emociones presentes en los tweets almacenados en Elasticsearch.
    
    - Procesa los tweets dentro del rango de fechas especificado
    - Aplica un modelo de clasificación de emociones a cada tweet
    - Actualiza los documentos en Elasticsearch con el análisis
    - Devuelve un resumen del procesamiento
    
    Los tweets pueden ser filtrados por:
    - Rango de fechas (start_date y end_date en formato YYYY-MM-DD)
    - Cantidad máxima de tweets a procesar (limit)
    """,
    response_description="Mensaje con el resumen del procesamiento"
)
async def analyze_emotions(
    start_date: Optional[str] = Query(
        None,
        description="Fecha de inicio para filtrar tweets (formato YYYY-MM-DD)",
        example="2024-03-14",
        regex="^\\d{4}-\\d{2}-\\d{2}$"
    ),
    end_date: Optional[str] = Query(
        None,
        description="Fecha final para filtrar tweets (formato YYYY-MM-DD)",
        example="2024-03-14",
        regex="^\\d{4}-\\d{2}-\\d{2}$"
    ),
    limit: Optional[int] = Query(
        None,
        description="Número máximo de tweets a procesar",
        example=100,
        gt=0
    )
):
    # Validar y ajustar fechas
    def adjust_date(date_str: Optional[str], is_end: bool = False) -> Optional[str]:
        if date_str is None:
            return None
        try:
            # Parsear la fecha
            date = datetime.strptime(date_str, "%Y-%m-%d")
            # Agregar tiempo (23:59:59 para end_date, 00:00:00 para start_date)
            if is_end:
                date = datetime.combine(date.date(), time(23, 59, 59))
            else:
                date = datetime.combine(date.date(), time(0, 0, 0))
            return date.isoformat()
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Formato de fecha inválido. Use el formato YYYY-MM-DD"
            )

    # Ajustar las fechas con el tiempo correspondiente
    start_datetime = adjust_date(start_date, False)
    end_datetime = adjust_date(end_date, True)

    try:
        es_client = get_es_client()
        start_time = time_lib.time()
        
        # Construir la consulta de Elasticsearch
        query = {"match_all": {}}
        if start_datetime or end_datetime:
            query = {
                "range": {
                    "meta.created_at": {
                        "gte": start_datetime if start_datetime else None,
                        "lte": end_datetime if end_datetime else None
                    }
                }
            }
        
        # Obtener tweets
        search_params = {
            "index": INDEX_NAME,
            "query": query,
            "_source": ["payload.tweet.content", "id", "metrics"],
            "size": limit if limit else 10000
        }
        
        logger.info(f"Buscando tweets con parámetros: {search_params}")
        result = es_client.search(**search_params)
        
        hits = result['hits']['hits']
        logger.info(f"Se encontraron {len(hits)} tweets")
        
        if not hits:
            return EmotionResponse(message="No se encontraron tweets en el rango de fechas especificado.")
        
        # Procesar tweets
        successful_updates = 0
        total_tweets = len(hits)
        
        for hit in hits:
            try:
                tweet = hit['_source']
                logger.info(f"Procesando tweet: {tweet}")
                content = tweet['payload']['tweet']['content']
                
                # Analizar emociones
                emotions = emotion_classifier(content)[0]
                top_emotion = max(emotions, key=lambda x: x['score'])
                
                # Preparar el documento para actualización
                doc = {
                    "metrics": tweet.get("metrics", {})
                }
                doc["metrics"]["emotion"] = top_emotion["label"]
                doc["emotion_analysis"] = {
                    "dominant_emotion": {
                        "label": top_emotion['label'],
                        "score": float(top_emotion['score'])
                    },
                    "all_emotions": [
                        {
                            "label": emotion['label'],
                            "score": float(emotion['score'])
                        }
                        for emotion in sorted(emotions, key=lambda x: x['score'], reverse=True)
                    ]
                }
                
                # Actualizar documento en Elasticsearch
                update_params = {
                    "index": INDEX_NAME,
                    "id": hit['_id'],
                    "body": {
                        "doc": doc
                    },
                    "refresh": True
                }
                
                logger.info(f"Actualizando tweet {hit['_id']} con emociones: {doc}")
                response = es_client.update(**update_params)
                logger.info(f"Respuesta de actualización: {response}")
                
                if response.get('result') == 'updated':
                    successful_updates += 1
                else:
                    logger.error(f"Error al actualizar tweet {hit['_id']}: {response}")
                
            except Exception as e:
                logger.error(f"Error procesando tweet {hit.get('_id', 'unknown')}: {str(e)}")
                continue
        
        execution_time = time_lib.time() - start_time
        
        if successful_updates == total_tweets:
            return EmotionResponse(
                message=f"Clasificación completada con éxito: {successful_updates} tweets procesados en {execution_time:.2f} segundos."
            )
        else:
            return EmotionResponse(
                message=f"Clasificación parcial: {successful_updates} de {total_tweets} tweets fueron procesados correctamente. Revisa los logs para más detalles."
            )
            
    except Exception as e:
        logger.error(f"Error en el endpoint: {str(e)}")
        if "ConnectionError" in str(e):
            raise HTTPException(status_code=503, detail={
                "message": "Error: no se pudo conectar con Elasticsearch."
            })
        raise HTTPException(status_code=500, detail={
            "message": f"Error inesperado durante el procesamiento: {str(e)}"
        }) 