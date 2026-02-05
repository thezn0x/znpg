import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """Simple logger for znpg"""
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(
        logging.Formatter(
            '[%(levelname)s] - %(asctime)s - %(filename)s : %(lineno)d: %(message)s',
            datefmt='%H:%M:%S'
        )
    )
    logger.addHandler(console)
    logger.propagate = False
    
    return logger