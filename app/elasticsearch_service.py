from elasticsearch import Elasticsearch, exceptions
from logger import setup_logger

logger = setup_logger("elasticsearch_service")

INDEX_NAME = "tweets"

ES_MAPPING = {
    "settings": {},
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "user": {
                "username": {"type": "keyword"},
                "handle": {"type": "keyword"},
                "verified": {"type": "boolean"}
            },
            "meta": {
                "created_at": {"type": "date"},
                "hashtags": {"type": "keyword"}
            },
            "payload": {
                "tweet": {
                    "content": {"type": "text"}
                }
            },
            "metrics": {
                "retweets": {"type": "long"},
                "likes": {"type": "long"},
                "emotion": {"type": "keyword"},
                "stance": {"type": "keyword"}
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


def ensure_index(es: Elasticsearch):
    try:
        if not es.indices.exists(index=INDEX_NAME):
            es.indices.create(index=INDEX_NAME, body=ES_MAPPING)
            logger.info(f"Índice '{INDEX_NAME}' creado exitosamente")
        else:
            logger.info(f"Índice '{INDEX_NAME}' ya existe")
    except Exception as e:
        logger.error(f"Error al crear/verificar índice: {str(e)}")
        raise


def index_tweet(es: Elasticsearch, tweet_data: dict):
    try:
        # Asegurarse de que los campos emotion y stance existan en metrics
        if "metrics" not in tweet_data:
            tweet_data["metrics"] = {}
        
        if "emotion" not in tweet_data["metrics"]:
            tweet_data["metrics"]["emotion"] = None
        
        if "stance" not in tweet_data["metrics"]:
            tweet_data["metrics"]["stance"] = None

        response = es.index(index=INDEX_NAME, document=tweet_data)
        logger.info(f"Tweet indexado exitosamente con ID: {response['_id']}")
        return response
    except Exception as e:
        logger.error(f"Error al indexar tweet: {str(e)}")
        raise
