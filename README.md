# Political-Tweets-ETL

Este proyecto implementa un sistema de ETL (Extract, Transform, Load) para tweets políticos, con capacidades de análisis de emociones y posturas (stance). La aplicación está construida con FastAPI y Elasticsearch.

## Estructura del Proyecto

El proyecto está organizado en varios grupos de archivos, cada uno con funcionalidades específicas:

### Componentes Principales (Core)

- **main.py**: Punto de entrada de la aplicación. Configura FastAPI, registra los routers, middleware y documentación Swagger.
- **logger.py**: Implementa un sistema de logging personalizado para toda la aplicación.
- **elasticsearch_service.py**: Proporciona servicios de conexión y operación con Elasticsearch, incluyendo inicialización de índices.

### Endpoints y Rutas (Routes)

- **routes/tweets.py**: Implementa el endpoint para obtener tweets con filtrado por fechas y paginación.
  - Permite filtrar tweets por rango de fechas (formato YYYY-MM-DD)
  - Soporta paginación mediante el parámetro `limit`
  - Estructura de respuesta estandarizada con metadatos

- **routes/emotion.py**: Implementa el endpoint para análisis de emociones en tweets.
  - Utiliza un modelo de clasificación de emociones basado en transformers
  - Procesa tweets dentro de un rango de fechas especificado
  - Actualiza documentos en Elasticsearch con los resultados del análisis

### Tests

- **tests/conftest.py**: Configuración de fixtures comunes para todos los tests.
- **tests/test_elasticsearch.py**: Tests para la conexión y operaciones básicas con Elasticsearch.
- **tests/test_emotion_analysis.py**: Tests para el análisis de emociones en textos.
- **tests/test_emotion_endpoint.py**: Tests para el endpoint de análisis de emociones.
- **tests/test_tweets_endpoint.py**: Tests para el endpoint de obtención de tweets.

## Características Principales

### Obtención de Tweets

- Filtrado por rango de fechas en formato YYYY-MM-DD
- Paginación mediante límite de resultados
- Estructura de respuesta consistente con metadatos

### Análisis de Emociones

- Utiliza un modelo pre-entrenado de `transformers` para clasificar las emociones
- Soporta filtrado por fechas y limitación del número de tweets
- Guarda resultados del análisis en Elasticsearch para consultas posteriores

### Validación y Manejo de Errores

- Validación de formatos de fechas (YYYY-MM-DD)
- Validación de límites para paginación
- Manejo adecuado de errores HTTP

## Modelos de Datos

### Tweet
```
{
  "id": string,
  "user": {
    "username": string,
    "handle": string,
    "verified": boolean
  },
  "content": string,
  "created_at": string,
  "metrics": {
    "likes": integer,
    "retweets": integer,
    "replies": integer,
    "emotion": string,
    "stance": string
  }
}
```

### Respuesta de Análisis de Emociones
```
{
  "message": string,
  "processed": integer
}
```

## Cambios Recientes

En la última actualización, se realizaron las siguientes mejoras:

1. **Formato de fechas**:
   - Cambiado el formato de ISO 8601 a YYYY-MM-DD para simplificar el uso.
   - Añadida conversión automática a hora inicio/fin del día para búsquedas más intuitivas.

2. **Estructura de usuario**:
   - Implementado un modelo UserInfo más completo con username, handle y verified.
   - Mejorado el procesamiento de información de usuario en los endpoints.

3. **Mejoras en tests**:
   - Ajustados los fixtures para asegurar que los datos de prueba sean consistentes.
   - Corregidos los tests para usar el nuevo formato de fechas y estructura de datos.

4. **Manejo de errores**:
   - Mejorada la propagación de errores HTTP para mejor información al cliente.
   - Añadido logging más detallado para facilitar la depuración.

## Requerimientos

- Python 3.11+
- FastAPI
- Elasticsearch 8.x
- Transformers (Hugging Face)
- Pytest (para tests)

## Despliegue

El proyecto incluye un archivo `docker-compose.yml` para facilitar el despliegue en entornos de desarrollo o producción.

```bash
docker-compose up -d
```
