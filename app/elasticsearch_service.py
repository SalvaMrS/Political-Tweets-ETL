from elasticsearch import Elasticsearch, exceptions
from logger import setup_logger
import json
from pathlib import Path
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

logger = setup_logger("elasticsearch_service")

# Obtener variables de entorno
INDEX_NAME = os.getenv("ES_INDEX")
ES_HOST = os.getenv("ES_HOST")
ES_USER = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")
ES_VERIFY_CERTS = os.getenv("ES_VERIFY_CERTS") == "true"
ES_SSL_WARN = os.getenv("ES_SSL_WARN") == "true"

# Verificar variables requeridas
if not all([INDEX_NAME, ES_HOST]):
    raise ValueError("Las variables ES_INDEX y ES_HOST son requeridas en el archivo .env")

ES_MAPPING = {
    "settings": {},
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


def get_es_client():
    try:
        auth = (ES_USER, ES_PASSWORD) if ES_USER and ES_PASSWORD else None
        es = Elasticsearch(
            hosts=[ES_HOST],
            verify_certs=ES_VERIFY_CERTS,
            basic_auth=auth,
            ssl_show_warn=ES_SSL_WARN
        )
        logger.info("Cliente de Elasticsearch creado exitosamente")
        return es
    except Exception as e:
        logger.error(f"Error al crear cliente de Elasticsearch: {str(e)}")
        raise


def load_initial_data(es: Elasticsearch):
    try:
        json_path = Path("tweets_dataset.json")
        if not json_path.exists():
            logger.warning("Archivo tweets_dataset.json no encontrado")
            return

        with open(json_path, 'r') as f:
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
    except Exception as e:
        logger.error(f"Error al cargar datos iniciales: {str(e)}")
        raise


def ensure_index(es: Elasticsearch):
    try:
        if not es.indices.exists(index=INDEX_NAME):
            es.indices.create(index=INDEX_NAME, body=ES_MAPPING)
            logger.info(f"Índice '{INDEX_NAME}' creado exitosamente")
            # Cargar datos iniciales solo si se creó el índice
            load_initial_data(es)
        else:
            logger.info(f"Índice '{INDEX_NAME}' ya existe")
    except Exception as e:
        logger.error(f"Error al crear/verificar índice: {str(e)}")
        raise


def index_tweet(es: Elasticsearch, tweet_data: dict):
    try:
        response = es.index(index=INDEX_NAME, document=tweet_data)
        logger.info(f"Tweet indexado exitosamente con ID: {response['_id']}")
        return response
    except Exception as e:
        logger.error(f"Error al indexar tweet: {str(e)}")
        raise
