import logging
from os import listdir
from os.path import isfile, join
import itertools
import re
from cubetl.core import Node
import time
from cubetl.functions.text import parsebool
from repoze.lru import LRUCache

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Cache():
    
    def __init__(self):
         
        self._cache = None
         
    def initialize(self): 
         
        if (self._cache == None):
         
            self._cache = LRUCache(512) # 100 max length
        
    def cache(self):
        self.initialize()
        return self._cache


