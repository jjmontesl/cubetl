from cubetl.core import Node, Component
import logging
from mako import template

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Template(Component):

    # TODO: Add generic support for both templates in files and templates in data (also interpolable)

    # TODO: (?) Have a generic way of loading data in components? (allow url magic for http, ftp, etc <- also in file reader / file line reader)
    # this would allow to retrieve CSVs, Templates... and process them either as a stream or load them to a component

    pass

