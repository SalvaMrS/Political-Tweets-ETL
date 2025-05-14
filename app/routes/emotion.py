"""
Endpoint para el análisis de emociones en tweets.
Proporciona funcionalidad para analizar y clasificar las emociones en tweets almacenados en Elasticsearch.
"""

from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, Dict, Any, List
from datetime import datetime, time
import time
from transformers import pipeline
from elasticsearch_service import get_es_client, INDEX_NAME
from pydantic import BaseModel, Field
import logging

# Configuración del logger
logger = logging.getLogger(__name__)

# Inicialización del router
router = APIRouter()

# Constantes
DEFAULT_LIMIT = 10000
DATE_FORMAT = "%Y-%m-%d"

# Inicializar el clasificador de emociones
emotion_classifier = pipeline(
    "text-classification",
    model="j-hartmann/emotion-english-distilroberta-base",
    return_all_scores=True
)

class EmotionRequest(BaseModel):
    """
    Modelo de request para el endpoint de emociones.
    
    Attributes:
        start_date (Optional[str]): Fecha de inicio para filtrar tweets (YYYY-MM-DD)
        end_date (Optional[str]): Fecha final para filtrar tweets (YYYY-MM-DD)
        limit (Optional[int]): Número máximo de tweets a procesar
    """
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    limit: Optional[int] = Field(None, gt=0)

class EmotionResponse(BaseModel):
    """
    Modelo de respuesta para el endpoint de emociones.
    
    Attributes:
        message (str): Mensaje descriptivo del resultado del procesamiento
        processed (int): Número de tweets procesados exitosamente
    """
    message: str
    processed: int

def validate_date(date_str: Optional[str], is_start: bool = True) -> None:
    """
    Valida el formato de una fecha en formato YYYY-MM-DD.
    
    Args:
        date_str (Optional[str]): Fecha a validar
        is_start (bool): Indica si es la fecha inicial (para mensaje de error)
        
    Raises:
        HTTPException: Si el formato de la fecha es inválido
    """
    if date_str:
        try:
            datetime.strptime(date_str, DATE_FORMAT)
        except ValueError:
            date_type = "inicial" if is_start else "final"
            raise HTTPException(
                status_code=422,
                detail=f"Formato de fecha {date_type} inválido. Use el formato YYYY-MM-DD"
            )

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
    response_description="Mensaje con el resumen del procesamiento y número de tweets procesados"
)
async def analyze_emotions(
    request: EmotionRequest = Body(
        None,
        description="Parámetros de filtrado para el análisis de emociones",
        example={
            "start_date": "2024-03-14",
            "end_date": "2024-03-15",
            "limit": 100
        }
    )
) -> EmotionResponse:
    """
    Analiza las emociones en los tweets almacenados en Elasticsearch.
    
    Args:
        request (EmotionRequest): Parámetros de filtrado para el análisis
        
    Returns:
        EmotionResponse: Mensaje con el resumen del procesamiento y número de tweets procesados
        
    Raises:
        HTTPException: Si hay errores en el formato de fechas o en el procesamiento
    """
    try:
        # Si no se proporciona un objeto de solicitud, crear uno vacío
        if request is None:
            request = EmotionRequest()
        
        # Validar formato de fechas
        validate_date(request.start_date, True)
        validate_date(request.end_date, False)
        
        es_client = get_es_client()
        start_time = time.time()
        
        # Construir la consulta de Elasticsearch
        query = {"match_all": {}}
        if request.start_date or request.end_date:
            range_query = {"range": {"meta.created_at": {}}}
            
            if request.start_date:
                range_query["range"]["meta.created_at"]["gte"] = f"{request.start_date}T00:00:00"
            
            if request.end_date:
                range_query["range"]["meta.created_at"]["lte"] = f"{request.end_date}T23:59:59"
            
            query = range_query
        
        logger.info(f"Consulta construida: {query}")
        
        # Obtener tweets
        search_params = {
            "index": INDEX_NAME,
            "query": query,
            "_source": ["payload.tweet.content", "id", "metrics", "meta", "user"],
            "size": request.limit if request.limit else DEFAULT_LIMIT
        }
        
        logger.info(f"Buscando tweets con parámetros: {search_params}")
        result = es_client.search(**search_params)
        
        hits = result['hits']['hits']
        logger.info(f"Se encontraron {len(hits)} tweets")
        
        if not hits:
            return EmotionResponse(
                message="No se encontraron tweets en el rango de fechas especificado.",
                processed=0
            )
        
        # Procesar tweets
        successful_updates = 0
        total_tweets = len(hits)
        
        for hit in hits:
            try:
                tweet = hit['_source']
                logger.info(f"Procesando tweet con ID: {hit['_id']}")
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
                    "doc": doc
                }
                
                logger.info(f"Actualizando tweet con ID: {hit['_id']}")
                es_client.update(**update_params)
                successful_updates += 1
                
            except Exception as e:
                logger.error(f"Error al procesar tweet {hit['_id']}: {str(e)}")
                continue
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        return EmotionResponse(
            message=f"Procesamiento completado. {successful_updates} de {total_tweets} tweets actualizados en {processing_time:.2f} segundos.",
            processed=successful_updates
        )
        
    except HTTPException as e:
        # Propagar errores HTTP directamente
        raise e
    except Exception as e:
        logger.error(f"Error en el procesamiento: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 