"""
Endpoint para obtener tweets con filtrado y paginación.
Proporciona funcionalidad para recuperar tweets de Elasticsearch con varios filtros.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from datetime import datetime
from elasticsearch_service import get_es_client, INDEX_NAME
from pydantic import BaseModel, Field
import logging

# Configuración del logger
logger = logging.getLogger(__name__)

# Inicialización del router
router = APIRouter()

# Constantes
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
DATE_FORMAT = "%Y-%m-%d"

class TweetMetrics(BaseModel):
    """
    Modelo para las métricas de un tweet.
    
    Attributes:
        likes (int): Número de likes
        retweets (int): Número de retweets
        replies (int): Número de respuestas
        emotion (Optional[str]): Emoción detectada
        stance (Optional[str]): Postura detectada
    """
    likes: int = 0
    retweets: int = 0
    replies: int = 0
    emotion: Optional[str] = None
    stance: Optional[str] = None

class UserInfo(BaseModel):
    """
    Modelo para la información del usuario.
    
    Attributes:
        username (str): Nombre de usuario
        handle (Optional[str]): Handle del usuario
        verified (Optional[bool]): Si el usuario está verificado
    """
    username: str
    handle: Optional[str] = None
    verified: Optional[bool] = False

class Tweet(BaseModel):
    """
    Modelo para un tweet.
    
    Attributes:
        id (str): ID del tweet
        user (UserInfo): Información del usuario
        content (str): Contenido del tweet
        created_at (str): Fecha de creación
        metrics (TweetMetrics): Métricas del tweet
    """
    id: str
    user: UserInfo
    content: str
    created_at: str
    metrics: TweetMetrics

class TweetsResponse(BaseModel):
    """
    Modelo de respuesta para el endpoint de tweets.
    
    Attributes:
        total (int): Número total de tweets
        tweets (List[Tweet]): Lista de tweets
    """
    total: int
    tweets: List[Tweet]

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

def build_date_query(start_date: Optional[str], end_date: Optional[str]) -> Dict[str, Any]:
    """
    Construye la consulta de rango de fechas para Elasticsearch.
    
    Args:
        start_date (Optional[str]): Fecha de inicio en formato YYYY-MM-DD
        end_date (Optional[str]): Fecha final en formato YYYY-MM-DD
        
    Returns:
        Dict[str, Any]: Consulta de rango de fechas
    """
    if not (start_date or end_date):
        return {"match_all": {}}
    
    range_query = {"range": {"meta.created_at": {}}}
    
    if start_date:
        range_query["range"]["meta.created_at"]["gte"] = f"{start_date}T00:00:00"
    
    if end_date:
        range_query["range"]["meta.created_at"]["lte"] = f"{end_date}T23:59:59"
    
    return range_query

def process_tweet(hit: Dict[str, Any]) -> Tweet:
    """
    Procesa un hit de Elasticsearch y lo convierte en un Tweet.
    
    Args:
        hit (Dict[str, Any]): Hit de Elasticsearch
        
    Returns:
        Tweet: Tweet procesado
    """
    source = hit["_source"]
    metrics = source.get("metrics", {})
    user_info = source.get("user", {})
    
    # Crear UserInfo con valores por defecto si algún campo falta
    user = UserInfo(
        username=user_info.get("username", "unknown"),
        handle=user_info.get("handle", None),
        verified=user_info.get("verified", False)
    )
    
    return Tweet(
        id=str(source["id"]),  # Asegurar que el ID sea string
        user=user,
        content=source["payload"]["tweet"]["content"],
        created_at=source["meta"]["created_at"],
        metrics=TweetMetrics(
            likes=metrics.get("likes", 0),
            retweets=metrics.get("retweets", 0),
            replies=metrics.get("replies", 0),
            emotion=metrics.get("emotion"),
            stance=metrics.get("stance")
        )
    )

@router.get(
    "/tweets",
    response_model=TweetsResponse,
    summary="Obtener tweets",
    description="""
    Obtiene tweets de Elasticsearch con filtrado y paginación.
    
    Los tweets pueden ser filtrados por:
    - Rango de fechas (start_date y end_date en formato YYYY-MM-DD)
    - Cantidad máxima de tweets a retornar (limit)
    
    Ejemplo de fechas válidas:
    - 2024-03-14
    - 2024-03-15
    """,
    response_description="Lista de tweets con el total de resultados"
)
async def get_tweets(
    start_date: Optional[str] = Query(
        None,
        description="Fecha de inicio para filtrar tweets (formato YYYY-MM-DD)",
        example="2024-03-14"
    ),
    end_date: Optional[str] = Query(
        None,
        description="Fecha final para filtrar tweets (formato YYYY-MM-DD)",
        example="2024-03-15"
    ),
    limit: int = Query(
        DEFAULT_LIMIT,
        description="Número máximo de tweets a retornar",
        example=100,
        gt=0,
        le=MAX_LIMIT
    )
) -> TweetsResponse:
    """
    Obtiene tweets de Elasticsearch con filtrado y paginación.
    
    Args:
        start_date (Optional[str]): Fecha de inicio para filtrar tweets (YYYY-MM-DD)
        end_date (Optional[str]): Fecha final para filtrar tweets (YYYY-MM-DD)
        limit (int): Número máximo de tweets a retornar
        
    Returns:
        TweetsResponse: Lista de tweets con el total de resultados
        
    Raises:
        HTTPException: Si hay errores en el formato de fechas o en la consulta
    """
    try:
        # Validar formato de fechas
        validate_date(start_date, True)
        validate_date(end_date, False)
        
        es_client = get_es_client()
        
        # Construir la consulta
        query = build_date_query(start_date, end_date)
        logger.info(f"Consulta construida: {query}")
        
        # Realizar la búsqueda
        search_params = {
            "index": INDEX_NAME,
            "query": query,
            "_source": ["id", "user", "payload.tweet.content", "meta.created_at", "metrics"],
            "size": limit
        }
        
        logger.info(f"Buscando tweets con parámetros: {search_params}")
        result = es_client.search(**search_params)
        
        # Procesar resultados
        hits = result["hits"]["hits"]
        total = result["hits"]["total"]["value"]
        
        tweets = [process_tweet(hit) for hit in hits]
        
        return TweetsResponse(
            total=total,
            tweets=tweets
        )
        
    except HTTPException as e:
        # Propagar errores HTTP directamente
        raise e
    except Exception as e:
        logger.error(f"Error al obtener tweets: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 