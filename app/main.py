from fastapi import FastAPI
from app.elasticsearch_service import get_es_client, ensure_index

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    es = get_es_client()
    ensure_index(es)
