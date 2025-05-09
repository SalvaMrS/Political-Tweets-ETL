from elasticsearch import Elasticsearch, exceptions

INDEX_NAME = "tweets"

ES_MAPPING = {
    "settings": {},
    "mappings": {
        "properties": {
            "id": {"type": "keyword"},
            "handle": {"type": "keyword"},
            "created_at": {"type": "date"},
            "hashtags": {"type": "keyword"},
            "content": {"type": "text"},
            "emotion": {"type": "keyword"},
            "stance": {"type": "keyword"}
        }
    }
}


def get_es_client():
    return Elasticsearch(
        hosts=["http://localhost:9200"],
        verify_certs=False,
        basic_auth=None,
        ssl_show_warn=False
    )


def ensure_index(es: Elasticsearch):
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, body=ES_MAPPING)
        print(f"Índice '{INDEX_NAME}' creado.")
    else:
        print(f"Índice '{INDEX_NAME}' ya existe.")
