import pandas as pd
from pandas import concat
from pprint import pprint
from HandlerComplete import  Handler, MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from ClassesComplete import NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map, Acquisition, Processing, Modelling, Optimising, Exporting


class BasicMashup(object):  #combining the results coming from different handlers
    def __init__(self):
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
                object = NauticalChart(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
                result.append(object)

            elif row["Type"] == "Manuscript plate":
                object = ManuscriptPlate(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])    
                result.append(object) 

            elif row["Type"] == "Manuscript volume":
                object = ManuscriptVolume(title=row['Title'], date=row['Date'], owner=row['Owner'], place=row['Place'], authos=row['Authors'])
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
        
        result = []

        for handler in self.processQuery:
            df = handler.getAllActivities()
            
            for _, row in df.iterrows():
                type, id = row["internalId"].split("-")

                if type == "acquisition":
                    object = Acquisition(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'], technique=row['technique'])
                    result.append(str(object))

                elif type == "processing":
                    object = Processing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                    result.append(str(object))
                
                elif type == "modelling":
                    object = Modelling(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                    result.append(str(object))
                
                elif type == "optimising":
                    object = Optimising(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                    result.append(str(object))

                elif type == "exporting":
                    object = Exporting(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                    result.append(str(object))    
                
        return result   

    def getActivitiesByResponsibleInstitution(self, institution):
        
        result = []

        for handler in self.processQuery:
            df = handler.getActivitiesByResponsibleInstitution(institution)

            for _, row in df.iterrows():
                type, id = row["internalID"].split("-")

                if type == "acquisition":
                    object = Acquisition(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'], technique=row['technique'])
                    result.append(str(object))

                elif type == "processing":
                    object = Processing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                    result.append(str(object))
                
                elif type == "modelling":
                    object = Modelling(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                    result.append(str(object))
                elif type == "optimising":
                    object = Optimising(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                    result.append(str(object))

                elif type == "exporting":
                    object = Exporting(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                    result.append(str(object))  
                
        return result

    def getActivitiesByResponsiblePerson(self, person):
        df = ProcessDataQueryHandler.getActivitiesByResponsiblePerson(self, person)
        result = []
        for _, row in df.iterrows():
            type, id = row["internalID"].split("-")

            if type == "acquisition":
                object = Acquisition(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'], technique=row['technique'])
                result.append(object)

            elif type == "processing":
                object = Processing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)
            
            elif type == "modelling":
                object = Modelling(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)
            
            elif type == "optimising":
                object = Optimising(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)

            elif type == "exporting":
                object = Exporting(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=row['objectId'])
                result.append(object)    
            
            return result


data = ProcessDataUploadHandler()
data.setDbPathOrUrl("database.db")
data.pushDataToDb("process.json")

process_query_handler = ProcessDataQueryHandler()
process_query_handler.setDbPathOrUrl("database.db")

mashup = BasicMashup()
mashup.addProcessHandler(process_query_handler)

result_list = mashup.getActivitiesByResponsibleInstitution("Philology")
pprint(result_list)

#instance of MatadataQueryHandler
#metadata_handler = MetadataQueryHandler()
#metadata_handler.setDbPathOrUrl("")   #missing the path of database

#add instance of MatadataQueryHandler to the BasicMashup
#mashup.addMetadataHandler(metadata_handler)

#instance of ProcessQueryHandler

#mashup.addProcessHandler(process_handler)        