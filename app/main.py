from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from elasticsearch_service import get_es_client, ensure_index, index_tweet
from logger import setup_logger
import time
import json
from pathlib import Path

app = FastAPI()
logger = setup_logger("api")

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    logger.info(
        f"Method: {request.method} Path: {request.url.path} "
        f"Status: {response.status_code} Duration: {process_time:.2f}s"
    )
    
    return response

@app.on_event("startup")
async def startup_event():
    try:
        es = get_es_client()
        ensure_index(es)
        logger.info("Aplicación iniciada correctamente")
    except Exception as e:
        logger.error(f"Error al iniciar la aplicación: {str(e)}")
        raise

@app.post("/load-tweets")
async def load_tweets():
    try:
        es = get_es_client()
        json_path = Path("tweets_dataset.json")
        
        if not json_path.exists():
            raise HTTPException(status_code=404, detail="Archivo tweets_dataset.json no encontrado")
        
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
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Error no manejado: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"message": "Error interno del servidor"}
    )
