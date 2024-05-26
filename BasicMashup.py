import pandas as pd
from pandas import concat
from pprint import pprint

class BasicMashup(object):
    def _init__(self):
        self.metadataQuery = []
        self.processQuery = []
    def cleanMetadataHandlers(self):
        self.metadataQuery = []
        return True
    def cleanProcessHandlers(self):
        return True
    def addMetadataHandler(self):
        return True
    def addProcessHandler(self):
        return True
    