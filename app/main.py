"""
Módulo principal de la aplicación FastAPI para el procesamiento y análisis de tweets políticos.
Este módulo configura la aplicación, maneja el ciclo de vida y define los endpoints principales.
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from elasticsearch_service import get_es_client, ensure_index, index_tweet
from logger import setup_logger
from routes.tweets import router as tweets_router
from routes.emotion import router as emotion_router
import time
import json
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Dict, Any

# Configuración del logger para la aplicación principal
logger = setup_logger("api")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestiona el ciclo de vida de la aplicación FastAPI.
    
    Esta función se ejecuta al iniciar y finalizar la aplicación:
    - Al iniciar: Configura la conexión con Elasticsearch y asegura que el índice exista
    - Al finalizar: Registra el cierre de la aplicación
    
    Args:
        app (FastAPI): Instancia de la aplicación FastAPI
        
    Raises:
        Exception: Si hay un error al inicializar Elasticsearch o crear el índice
    """
    # Inicialización de la aplicación
    try:
        es = get_es_client()
        ensure_index(es)
        logger.info("Aplicación iniciada correctamente")
    except Exception as e:
        logger.error(f"Error al iniciar la aplicación: {str(e)}")
        raise
    yield
    # Finalización de la aplicación
    logger.info("Aplicación finalizada")

# Creación de la aplicación FastAPI con el manejador de ciclo de vida
app = FastAPI(
    title="API de Análisis de Tweets Políticos",
    description="API para el procesamiento y análisis de tweets políticos",
    version="1.0.0",
    lifespan=lifespan
)

# Registro de routers para diferentes funcionalidades
app.include_router(tweets_router, prefix="/api/v1", tags=["tweets"])
app.include_router(emotion_router, prefix="/api/v1", tags=["emotion"])

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Middleware para registrar información detallada de cada petición HTTP.
    
    Registra el método, ruta, código de estado y tiempo de procesamiento
    de cada petición que recibe la aplicación.
    
    Args:
        request (Request): Objeto de petición FastAPI
        call_next: Función para continuar el procesamiento de la petición
        
    Returns:
        Response: Respuesta HTTP procesada
    """
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    logger.info(
        f"Method: {request.method} Path: {request.url.path} "
        f"Status: {response.status_code} Duration: {process_time:.2f}s"
    )
    
    return response

@app.post("/api/v1/load-tweets", tags=["tweets"])
async def load_tweets() -> Dict[str, str]:
    """
    Endpoint para cargar tweets desde un archivo JSON a Elasticsearch.
    
    Lee tweets desde el archivo tweets_dataset.json y los indexa en Elasticsearch.
    Maneja errores individuales por tweet sin detener el proceso completo.
    
    Returns:
        Dict[str, str]: Mensaje con el resultado de la operación
        
    Raises:
        HTTPException: 
            - 404: Si no se encuentra el archivo de tweets
            - 500: Si ocurre un error durante el proceso de carga
    """
    try:
        es = get_es_client()
        json_path = Path("tweets_dataset.json")
        
        if not json_path.exists():
            raise HTTPException(
                status_code=404,
                detail="Archivo tweets_dataset.json no encontrado"
            )
        
        with open(json_path, 'r') as f:
            tweets = json.load(f)
        
        indexed_count = 0
        for tweet in tweets:
            try:
                index_tweet(es, tweet)
                indexed_count += 1
            except Exception as e:
                logger.error(f"Error al indexar tweet {tweet.get('id')}: {str(e)}")
                continue
        
        logger.info(f"Se indexaron {indexed_count} tweets exitosamente")
        return {"message": f"Se indexaron {indexed_count} tweets exitosamente"}
    
    except Exception as e:
        logger.error(f"Error al cargar tweets: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Manejador global de excepciones para la aplicación.
    
    Captura todas las excepciones no manejadas y las registra en el log.
    Las excepciones HTTPException son manejadas por FastAPI por defecto.
    
    Args:
        request (Request): Objeto de petición FastAPI
        exc (Exception): Excepción capturada
        
    Returns:
        JSONResponse: Respuesta HTTP con código 500 y mensaje de error
    """
    if isinstance(exc, HTTPException):
        raise exc
        
    logger.error(f"Error no manejado: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"message": "Error interno del servidor"}
    )
