"""
Módulo de servicio para la interacción con Elasticsearch.
Proporciona funciones para la gestión del índice y operaciones CRUD con tweets.
"""

from elasticsearch import Elasticsearch, exceptions
from logger import setup_logger
import json
from pathlib import Path
import os
from dotenv import load_dotenv
from typing import Dict, Any, Optional

# Cargar variables de entorno
load_dotenv()

# Configuración del logger
logger = setup_logger("elasticsearch_service")

# Constantes de configuración
INDEX_NAME = os.getenv("ES_INDEX")
ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")
ES_VERIFY_CERTS = os.getenv("ES_VERIFY_CERTS") == "true"
ES_SSL_WARN = os.getenv("ES_SSL_WARN") == "true"

# Verificar variables requeridas
if not all([INDEX_NAME, ES_HOST]):
    raise ValueError("Las variables ES_INDEX y ES_HOST son requeridas en el archivo .env")

# Mapeo del índice de Elasticsearch
ES_MAPPING = {
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "user": {
                "type": "object",
                "properties": {
                    "username": {"type": "keyword"},
                    "handle": {"type": "keyword"},
                    "verified": {"type": "boolean"}
                }
            },
            "meta": {
                "type": "object",
                "properties": {
                    "created_at": {"type": "date"},
                    "hashtags": {"type": "keyword"}
                }
            },
            "payload": {
                "type": "object",
                "properties": {
                    "tweet": {
                        "type": "object",
                        "properties": {
                            "content": {"type": "text"}
                        }
                    }
                }
            },
            "metrics": {
                "type": "object",
                "properties": {
                    "retweets": {"type": "long"},
                    "likes": {"type": "long"},
                    "emotion": {"type": "keyword"},
                    "stance": {"type": "keyword"}
                }
            }
        }
    }
}

def get_es_client() -> Elasticsearch:
    """
    Crea y retorna un cliente de Elasticsearch configurado.
    
    Esta función configura un cliente de Elasticsearch con los parámetros
    definidos en las variables de entorno. Maneja la autenticación básica
    si se proporcionan credenciales.
    
    Returns:
        Elasticsearch: Cliente de Elasticsearch configurado
        
    Raises:
        ConnectionError: Si no se puede establecer conexión con Elasticsearch
        ValueError: Si la configuración es inválida
    """
    try:
        auth = (ES_USER, ES_PASSWORD) if ES_USER and ES_PASSWORD else None
        es = Elasticsearch(
            hosts=[ES_HOST],
            verify_certs=ES_VERIFY_CERTS,
            basic_auth=auth,
            ssl_show_warn=ES_SSL_WARN,
        )
        
        # Verificar conexión
        if not es.ping():
            raise ConnectionError("No se pudo conectar con Elasticsearch")
            
        logger.info("Cliente de Elasticsearch creado exitosamente")
        return es
    except exceptions.ConnectionError as e:
        logger.error(f"Error de conexión con Elasticsearch: {str(e)}")
        raise ConnectionError(f"No se pudo conectar con Elasticsearch: {str(e)}")
    except Exception as e:
        logger.error(f"Error al crear cliente de Elasticsearch: {str(e)}")
        raise

def load_initial_data(es: Elasticsearch) -> None:
    """
    Carga datos iniciales de tweets desde un archivo JSON.
    
    Esta función lee tweets desde el archivo tweets_dataset.json y los indexa
    en Elasticsearch. Maneja errores individuales por tweet sin detener
    el proceso completo.
    
    Args:
        es (Elasticsearch): Cliente de Elasticsearch configurado
        
    Raises:
        FileNotFoundError: Si no se encuentra el archivo de tweets
        json.JSONDecodeError: Si el archivo JSON está mal formateado
    """
    try:
        json_path = Path("tweets_dataset.json")
        if not json_path.exists():
            logger.warning("Archivo tweets_dataset.json no encontrado")
            return

        with open(json_path, 'r', encoding='utf-8') as f:
            tweets = json.load(f)

        indexed_count = 0
        for tweet in tweets:
            try:
                es.index(index=INDEX_NAME, document=tweet)
                indexed_count += 1
            except Exception as e:
                logger.error(f"Error al indexar tweet {tweet.get('id')}: {str(e)}")
                continue

        logger.info(f"Se cargaron {indexed_count} tweets exitosamente")
    except FileNotFoundError:
        logger.error("Archivo tweets_dataset.json no encontrado")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error al decodificar JSON: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error al cargar datos iniciales: {str(e)}")
        raise

def ensure_index(es: Elasticsearch) -> None:
    """
    Verifica y crea el índice en Elasticsearch si no existe.
    
    Esta función verifica si el índice existe y lo crea si es necesario.
    Si el índice se crea, también carga los datos iniciales.
    
    Args:
        es (Elasticsearch): Cliente de Elasticsearch configurado
        
    Raises:
        exceptions.RequestError: Si hay un error en la creación del índice
        exceptions.ConnectionError: Si hay un error de conexión
    """
    try:
        if not es.indices.exists(index=INDEX_NAME):
            es.indices.create(index=INDEX_NAME, body=ES_MAPPING)
            logger.info(f"Índice '{INDEX_NAME}' creado exitosamente")
            # Cargar datos iniciales solo si se creó el índice
            load_initial_data(es)
        else:
            logger.info(f"Índice '{INDEX_NAME}' ya existe")
    except exceptions.RequestError as e:
        logger.error(f"Error al crear índice: {str(e)}")
        raise
    except exceptions.ConnectionError as e:
        logger.error(f"Error de conexión: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error al crear/verificar índice: {str(e)}")
        raise

def index_tweet(es: Elasticsearch, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Indexa un tweet en Elasticsearch.
    
    Esta función indexa un tweet en el índice configurado de Elasticsearch.
    El tweet debe tener la estructura definida en el mapeo del índice.
    
    Args:
        es (Elasticsearch): Cliente de Elasticsearch configurado
        tweet_data (Dict[str, Any]): Datos del tweet a indexar
        
    Returns:
        Dict[str, Any]: Respuesta de Elasticsearch con el resultado de la operación
        
    Raises:
        exceptions.RequestError: Si hay un error en la indexación
        exceptions.ValidationError: Si los datos del tweet no son válidos
    """
    try:
        if not isinstance(tweet_data, dict):
            raise ValueError("tweet_data debe ser un diccionario")
        response = es.index(index=INDEX_NAME, document=tweet_data)
        logger.info(f"Tweet indexado exitosamente con ID: {response['_id']}")
        return response
    except exceptions.RequestError as e:
        logger.error(f"Error al indexar tweet: {str(e)}")
        raise
    except exceptions.ValidationError as e:
        logger.error(f"Error de validación en datos del tweet: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado al indexar tweet: {str(e)}")
        raise
