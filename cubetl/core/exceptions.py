import logging

# Get an instance of a logger
logger = logging.getLogger(__name__)


class ETLException(Exception):
    pass


class ETLConfigurationException(ETLException):
    pass
