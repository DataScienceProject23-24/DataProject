import pandas as pd
from pandas import concat
from pprint import pprint
import HandlerComplete
from HandlerComplete import Handler


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
    
    def getCulturalHeritageObjectsAuthoredBy(self, authorId):
        df = pd.getCulturalHeritageObjectsAuthoredBy(self, authorId)
        result = []
        for _, row in df.iterrows():

            if row["Type"] == "Nautical chart":
                object = NauticalChart(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Manuscript plate":
                object = ManuscriptPlate(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])    
                result.append(object) 

            elif row["Type"] == "Manuscript volume":
                object =  ManuscriptVolume(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Printed volume":
                object = PrintedVolume(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Printed material":
                object = PrintedMaterial(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Herbarium":
                object = Herbarium(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Specimen":
                object = Specimen(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Painting":
                object = Painting(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Model":
                object = Model(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Map":
                object = Map(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)    

            return result                                    
    
    def getAllActivities(self):
        df = pd.getAllActivities(self, authorId)
        result = []
        for _, row in df.iterrows():

    def getActivitiesByResponsibleInstitution(self, institution):
        df = pd. getActivitiesByResponsibleInstitution(self, institution)
        result = []
        for _, row in df.iterrows():

    def getActivitiesByResponsiblePerson(self, person):
        df = pd.getActivitiesByResponsiblePerson(self, person)
        result = []
        for _, row in df.iterrows():