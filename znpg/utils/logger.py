import logging

def get_logger(name:str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s] - %(asctime)s - %(filename)s : %(lineno)d: %(message)s')
    file_handler = logging.file_handler(file="znpg/utils/logs.log")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.console_handler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger