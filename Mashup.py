import pandas as pd
from pandas import concat
from pprint import pprint


class BasicMashup(object):  #combining the results coming from different handlers
    def _init__(self):
        self.metadataQuery = []  #list of objects (handlers) of MetadataQueryHandler
        self.processQuery = [] #list of objects ProcessDataQueryHandler

    def cleanMetadataHandlers(self):
        self.metadataQuery = [] #clean the list metadataQuery
        return True
    
    def cleanProcessHandlers(self):
        self.processQuery = [] #clean the list processQuery
        return True
    
    def addMetadataHandler(self, metadataHandler): #it append one object in the list?
        self.metadataQuery.append(metadataHandler)   
        return True
    
    def addProcessHandler(self, processHandler):
        self.processQuery.append(processHandler)
        return True  

mashup= BasicMashup()

#instance of MatadataQueryHandler
metadata_handler = MatadataQueryHandler()
metadata_handler.setDbPathOrUrl("")   #missing the path of database

#add instance of MatadataQueryHandler to the BasicMashup
mashup.addMetadataHandler(metadata_handler)

#instance of ProcessQueryHandler
process_handler = ProcessDataQueryHandler()
process_handler.setDbPathOrUrl("")

mashup.addProcessHandler(process_handler)




    

