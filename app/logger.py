import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Crear directorio de logs si no existe
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

# Configuración del logger
def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Formato del log
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Handler para archivo de logs
    file_handler = RotatingFileHandler(
        log_dir / "api.log",
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # Handler para errores
    error_file_handler = RotatingFileHandler(
        log_dir / "error.log",
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    error_file_handler.setFormatter(formatter)
    error_file_handler.setLevel(logging.ERROR)

    # Handler para consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Agregar handlers al logger
    logger.addHandler(file_handler)
    logger.addHandler(error_file_handler)
    logger.addHandler(console_handler)

    return logger 