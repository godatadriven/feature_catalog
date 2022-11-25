import logging

def get_logger() -> logging.Logger:
    logging.basicConfig(level=logging.WARNING)
    return logging.getLogger()

class MissingColumnError(Exception):
    pass
