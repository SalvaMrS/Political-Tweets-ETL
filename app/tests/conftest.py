"""
Configuración global para los tests.
Inicializa Elasticsearch y configura fixtures comunes.
"""

import pytest
from elasticsearch_service import get_es_client, ensure_index, INDEX_NAME
import os
from pathlib import Path

def pytest_sessionstart(session):
    """
    Hook que se ejecuta al inicio de la sesión de pruebas.
    Configura el entorno de pruebas y asegura que Elasticsearch esté listo.
    """
    # Asegurar que estamos en el directorio correcto
    os.chdir(Path(__file__).parent.parent)
    
    # Inicializar Elasticsearch
    es = get_es_client()
    ensure_index(es)

@pytest.fixture(scope="session")
def es_client():
    """
    Fixture que proporciona un cliente de Elasticsearch configurado.
    Se crea una vez por sesión de pruebas.
    
    Returns:
        Elasticsearch: Cliente de Elasticsearch configurado
    """
    return get_es_client() 