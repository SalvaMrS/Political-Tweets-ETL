from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from datetime import datetime
from elasticsearch_service import get_es_client
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/tweets")
async def get_tweets(
    start_date: str = Query(None, description="Fecha inicial en formato ISO 8601 (YYYY-MM-DD)"),
    end_date: str = Query(None, description="Fecha final en formato ISO 8601 (YYYY-MM-DD)"),
    limit: int = Query(10, ge=1, le=100, description="Número máximo de tweets a retornar"),
    offset: int = Query(0, ge=0, description="Número de tweets a saltar")
):
    try:
        es = get_es_client()
        
        # Validar formato de fechas
        if start_date:
            try:
                datetime.fromisoformat(start_date)
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de fecha inicial inválido. Use ISO 8601")
        
        if end_date:
            try:
                datetime.fromisoformat(end_date)
            except ValueError:
                raise HTTPException(status_code=400, detail="Formato de fecha final inválido. Use ISO 8601")
        
        # Construir query
        query = {
            "bool": {
                "must": []
            }
        }
        
        # Agregar filtros de fecha si se proporcionan
        if start_date or end_date:
            date_filter = {"range": {"meta.created_at": {}}}
            if start_date:
                date_filter["range"]["meta.created_at"]["gte"] = start_date
            if end_date:
                date_filter["range"]["meta.created_at"]["lte"] = end_date
            query["bool"]["must"].append(date_filter)
        
        # Si no hay filtros, usar match_all
        if not query["bool"]["must"]:
            query = {"match_all": {}}
        
        # Realizar búsqueda
        response = es.search(
            index="tweets",
            query=query,
            size=limit,
            from_=offset,
            sort=[{"meta.created_at": "desc"}]
        )
        
        # Procesar resultados
        tweets = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            tweet = {
                "id": source["id"],
                "user": source["user"],
                "content": source["payload"]["tweet"]["content"],
                "created_at": source["meta"]["created_at"],
                "hashtags": source["meta"].get("hashtags", []),
                "metrics": {
                    "likes": source["metrics"].get("likes", 0),
                    "retweets": source["metrics"].get("retweets", 0)
                },
                "emotion": source["metrics"].get("emotion"),
                "stance": source["metrics"].get("stance")
            }
            tweets.append(tweet)
        
        return {
            "total": response["hits"]["total"]["value"],
            "tweets": tweets
        }
        
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        logger.error(f"Error al obtener tweets: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 