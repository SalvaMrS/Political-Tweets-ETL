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
app.include_router(tweets_router, tags=["tweets"])
app.include_router(emotion_router, tags=["emotion"])

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
