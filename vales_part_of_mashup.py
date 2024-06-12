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

        result = []

        for handler in self.metadataQuery:
            df = handler.getCulturalHeritageObjectsAuthoredBy(authorId) 

            for _, row in df.iterrows():

                type = row['type']

                if "NauticalChart" in type:
                    object = NauticalChart(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "ManuscriptPlate" in type:
                    object = ManuscriptPlate(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])    
                    result.append(object) 

                elif "ManuscriptVolume" in type:
                    object = ManuscriptVolume(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "PrintedVolume" in type:
                    object = PrintedVolume(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "PrintedMaterial" in type:
                    object = PrintedMaterial(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Herbarium" in type:
                    object = Herbarium(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Specimen" in type:
                    object = Specimen(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Painting" in type:
                    object = Painting(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Model" in type:
                    object = Model(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Map" in type:
                    object = Map(title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
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

    def getActivitiesByResponsibleInstitution(self, institution):
        
        result = []

        for handler in self.processQuery:
            df = handler.getActivitiesByResponsibleInstitution(institution)

            for _, row in df.iterrows():
                type, id = row["internalId"].split("-")

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

    def getActivitiesByResponsiblePerson(self, person):
        
        result = []

        for handler in self.processQuery:
            df = handler.getActivitiesByResponsiblePerson(person)

            for _, row in df.iterrows():
                type, id = row["internalId"].split("-")

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

'''
data = ProcessDataUploadHandler()
data.setDbPathOrUrl("database.db")
data.pushDataToDb("process.json")

process_query_handler = ProcessDataQueryHandler()
process_query_handler.setDbPathOrUrl("database.db")
'''

data2 = MetadataUploadHandler()
data2.setDbPathOrUrl("http://10.201.3.91:9999/blazegraph/sparql")
data2.pushDataToDb("meta.csv")

metadata_query_handler = MetadataQueryHandler()
metadata_query_handler.setDbPathOrUrl("http://10.201.3.91:9999/blazegraph/sparql")  

mashup = BasicMashup()
mashup.addMetadataHandler(metadata_query_handler)

result_list = mashup.getCulturalHeritageObjectsAuthoredBy("ULAN:500114874")
pprint(result_list)

#instance of MatadataQueryHandler
#metadata_handler = MetadataQueryHandler()
#metadata_handler.setDbPathOrUrl("")   #missing the path of database

#add instance of MatadataQueryHandler to the BasicMashup
#mashup.addMetadataHandler(metadata_handler)

#instance of ProcessQueryHandler

#mashup.addProcessHandler(process_handler)        