import pandas as pd
from pandas import concat
from pprint import pprint
from HandlerComplete import  Handler, MetadataQueryHandler, ProcessDataQueryHandler
from ClassesComplete import CulturalHeritageObject, Activity


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
        df = MetadataQueryHandler.getCulturalHeritageObjectsAuthoredBy(self, authorId)
        result = []
        for _, row in df.iterrows():

            if row["Type"] == "Nautical chart":
                object = CulturalHeritageObject.NauticalChart(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Manuscript plate":
                object = CulturalHeritageObject.ManuscriptPlate(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])    
                result.append(object) 

            elif row["Type"] == "Manuscript volume":
                object =  CulturalHeritageObject.ManuscriptVolume(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Printed volume":
                object = CulturalHeritageObject.PrintedVolume(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Printed material":
                object = CulturalHeritageObject.PrintedMaterial(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Herbarium":
                object = CulturalHeritageObject.Herbarium(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Specimen":
                object = CulturalHeritageObject.Specimen(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Painting":
                object = CulturalHeritageObject.Painting(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Model":
                object = CulturalHeritageObject.Model(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Map":
                object = CulturalHeritageObject.Map(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)    

            return result                                    
    
    def getAllActivities(self):
        df = ProcessDataQueryHandler.getAllActivities(self)
        result = []
        for _, row in df.iterrows():
            type, id = row["internalID"].split("-")

            if type == "acquisition":
                object = Activity.Acquisition(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'], technique=row['technique'])
                result.append(object)

            elif type == "processing":
                object = Activity.Processing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)
            
            elif type == "modelling":
                object = Activity.Modelling(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)
            
            elif type == "optimizing":
                object = Activity.Optimizing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)

            elif type == "exporting":
                object = Activity.Exporting(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)    
            
            return result


    def getActivitiesByResponsibleInstitution(self, institution):
        df = ProcessDataQueryHandler.getActivitiesByResponsibleInstitution(self, institution)
        result = []
        for _, row in df.iterrows():
            type, id = row["internalID"].split("-")

            if type == "acquisition":
                object = Activity.Acquisition(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'], technique=row['technique'])
                result.append(object)

            elif type == "processing":
                object = Activity.Processing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)
            
            elif type == "modelling":
                object = Activity.Modelling(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)
            
            elif type == "optimizing":
                object = Activity.Optimizing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)

            elif type == "exporting":
                object = Activity.Exporting(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)    
            
            return result

    def getActivitiesByResponsiblePerson(self, person):
        df = pd.getActivitiesByResponsiblePerson(self, person)
        result = []
        for _, row in df.iterrows():
            type, id = row["internalID"].split("-")

            if type == "acquisition":
                object = Activity.Acquisition(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'], technique=row['technique'])
                result.append(object)

            elif type == "processing":
                object = Activity.Processing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)
            
            elif type == "modelling":
                object = Activity.Modelling(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)
            
            elif type == "optimizing":
                object = Activity.Optimizing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)

            elif type == "exporting":
                object = Activity.Exporting(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)    
            
            return result