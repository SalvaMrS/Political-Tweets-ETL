from elasticsearch import Elasticsearch, exceptions
from logger import setup_logger
import json
from pathlib import Path

logger = setup_logger("elasticsearch_service")

INDEX_NAME = "tweets"

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
        es = Elasticsearch(
            hosts=["http://localhost:9200"],
            verify_certs=False,
            basic_auth=None,
            ssl_show_warn=False
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
