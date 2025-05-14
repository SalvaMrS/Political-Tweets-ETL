"""
Módulo de configuración de logging para la aplicación.
Proporciona una configuración centralizada para el registro de eventos y errores.
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional

# Constantes de configuración
LOG_DIR = Path("logs")
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
MAX_BYTES = 10 * 1024 * 1024  # 10MB en bytes
BACKUP_COUNT = 5
DEFAULT_LOG_LEVEL = logging.INFO
ERROR_LOG_LEVEL = logging.ERROR

# Crear directorio de logs si no existe
LOG_DIR.mkdir(exist_ok=True)

def setup_logger(
    name: str,
    log_level: Optional[int] = None,
    log_format: Optional[str] = None
) -> logging.Logger:
    """
    Configura y retorna un logger con handlers para archivo, errores y consola.
    
    Esta función configura un logger con tres handlers:
    1. Un handler para logs generales (api.log)
    2. Un handler específico para errores (error.log)
    3. Un handler para la consola
    
    Los archivos de log utilizan rotación automática cuando alcanzan el tamaño máximo.
    
    Args:
        name (str): Nombre del logger, típicamente el nombre del módulo
        log_level (Optional[int]): Nivel de logging para handlers generales.
                                  Por defecto usa logging.INFO
        log_format (Optional[str]): Formato personalizado para los mensajes de log.
                                   Por defecto usa el formato estándar
    
    Returns:
        logging.Logger: Logger configurado con todos los handlers necesarios
        
    Example:
        >>> logger = setup_logger("mi_modulo")
        >>> logger.info("Mensaje de información")
        >>> logger.error("Mensaje de error")
    """
    # Crear y configurar el logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level or DEFAULT_LOG_LEVEL)
    
    # Evitar duplicación de handlers
    if logger.handlers:
        return logger
    
    # Configurar el formateador
    formatter = logging.Formatter(log_format or LOG_FORMAT)
    
    # Configurar handler para logs generales
    file_handler = RotatingFileHandler(
        LOG_DIR / "api.log",
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level or DEFAULT_LOG_LEVEL)
    
    # Configurar handler para errores
    error_file_handler = RotatingFileHandler(
        LOG_DIR / "error.log",
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding='utf-8'
    )
    error_file_handler.setFormatter(formatter)
    error_file_handler.setLevel(ERROR_LOG_LEVEL)
    
    # Configurar handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level or DEFAULT_LOG_LEVEL)
    
    # Agregar handlers al logger
    logger.addHandler(file_handler)
    logger.addHandler(error_file_handler)
    logger.addHandler(console_handler)
    
    return logger 