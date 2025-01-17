import logging
import logging.handlers
import os

def setup_logger(name="cbl_migrator", log_file="cbl_migrator.log", level=logging.DEBUG):
    """Configure and return a logger with both file and console handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler with rotation
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create formatters and add them to the handlers
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    file_handler.setFormatter(file_formatter)
    console_handler.setFormatter(console_formatter)

    # Add the handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Create the default logger instance
logger = setup_logger()
