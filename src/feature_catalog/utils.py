import logging


def get_logger() -> logging.Logger:
    logging.basicConfig(level=logging.WARNING)
    return logging.getLogger()


class MissingColumnError(Exception):
    pass


class UnsupportedAggregationLevel(Exception):
    pass


class UnsupportedDependencies(Exception):
    pass


class UnsupportedFeatureName(Exception):
    pass
