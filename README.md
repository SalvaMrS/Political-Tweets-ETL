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

### Análisis de Datos y Emociones

En los notebooks se realizaron las siguientes actividades:

- **Carga de Datos**: Se cargaron los tweets desde un archivo CSV, asegurando que se mantuvieran todos los datos originales para su posterior análisis.

- **Limpieza de Datos**: Se implementaron procesos para eliminar duplicados y manejar valores nulos, garantizando la calidad de los datos antes de su análisis.

- **Análisis de Emociones**: Se utilizó un modelo de clasificación de emociones basado en transformers para analizar los tweets. Este modelo permitió identificar emociones como alegría, tristeza, enojo, entre otras, proporcionando una comprensión más profunda del sentimiento detrás de los tweets.

- **Visualización de Datos**: Se generaron gráficos y visualizaciones para representar las emociones y tendencias en los tweets, facilitando la interpretación de los resultados.

- **Exportación de Resultados**: Finalmente, los resultados del análisis se exportaron a un nuevo archivo CSV para su uso posterior.

### Análisis de Emociones

- Utiliza un modelo pre-entrenado de `transformers` para clasificar las emociones
- Soporta filtrado por fechas y limitación del número de tweets
- Guarda resultados del análisis en Elasticsearch para consultas posteriores

### Validación y Manejo de Errores

- Validación de formatos de fechas (YYYY-MM-DD)
- Validación de límites para paginación
- Manejo adecuado de errores HTTP

### Informe de Notas

**Informe de Notas**

**Ingesta y almacenamiento**
- Se cargó toda la información de los tweets directamente en un índice de Elasticsearch (tweets). Esta decisión se tomó para conservar todos los datos originales y poder explorarlos o reutilizarlos en el futuro sin perder contexto.

- No se realizó ningún proceso de limpieza o normalización de los datos previo a su almacenamiento. La idea fue mantener la integridad original del contenido tal como fue extraído.

**Clasificación de emociones**
- Se implementó un endpoint /classify que recibe un tweet y devuelve su emoción clasificada utilizando un modelo de Hugging Face (j-hartmann/emotion-english-distilroberta-base).

- El modelo resultó ser suficientemente preciso para un prototipo y demostró ser fácil de integrar con FastAPI.

**Detección de postura (Stance Detection)**
- Se intentó implementar un endpoint adicional para detectar la postura del autor del tweet frente al tema. La idea era clasificar el texto como "a favor", "en contra" o "neutral".

- Sin embargo, surgieron dos limitantes:

  - Capacidad computacional: Cargar modelos grandes (como LLaMA) de forma local resultó inviable con los recursos disponibles.

  - Tiempo: Para obtener resultados confiables, era necesario realizar fine-tuning o ajustar un modelo existente, lo cual no fue posible dentro del plazo.

**Contenedores y entorno de ejecución**
- Se diseñó una solución contenida en Docker. Primero se levantó Elasticsearch en un contenedor individual para pruebas.

- Luego, se desarrolló un contenedor unificado con la API de FastAPI y Elasticsearch, simplificando la ejecución y garantizando que todo el sistema pueda correrse localmente sin dependencias externas.

**Desafíos encontrados**
- Uno de los principales retos fue realizar tests para los endpoints de FastAPI, especialmente aquellos que dependían de la conexión a Elasticsearch. Hubo varios errores relacionados con la conexión, tiempos de espera y sincronización de los servicios durante los tests automatizados.

- También hubo dificultades iniciales con la compatibilidad de algunas librerías con Python 3.11, pero se resolvieron especificando las versiones exactas en requirements.txt.

**Posibles mejoras con más tiempo**
- Realizaría un fine-tuning de un modelo como LLaMA sobre un dataset de tweets políticos etiquetados para mejorar el rendimiento en la tarea de stance detection.

- Otra opción sería generar datos sintéticos que simulen interacciones políticas para entrenar una IA adaptada al dominio sin necesidad de usar modelos grandes directamente.

- Implementaría un sistema de monitoring y logging estructurado para tener trazabilidad en la API y facilitar el debugging en producción.

- Agregaría una capa de preprocesamiento opcional para normalizar textos, eliminar ruido y mejorar la entrada al modelo.

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

## Despliegue
El proyecto incluye un archivo `docker-compose.yml` para facilitar el despliegue en entornos de desarrollo o producción.

### Instrucciones para Iniciar el Proyecto

1. Asegúrate de tener Docker y Docker Compose instalados en tu máquina.
2. Clona el repositorio y navega a la carpeta del proyecto:
   
   ```bash
   git clone <URL_DEL_REPOSITORIO>
   cd Political-Tweets-ETL
   ```
3. Construye y levanta los contenedores:
   
   ```bash
   docker-compose up -d
   ```
4. Accede a la API en `http://localhost:8000` y a Elasticsearch en `http://localhost:9200`. Puedes acceder a la documentacion de la API en `http://localhost:8000/docs`