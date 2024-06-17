import pandas as pd
from pandas import concat
from pprint import pprint
from HandlerComplete import  Handler, MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from ClassesComplete import IdentifiableEntity, CulturalHeritageObject, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map, Acquisition, Processing, Modelling, Optimising, Exporting


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
                    object = NauticalChart(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "ManuscriptPlate" in type:
                    object = ManuscriptPlate(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])    
                    result.append(object) 

                elif "ManuscriptVolume" in type:
                    object = ManuscriptVolume(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "PrintedVolume" in type:
                    object = PrintedVolume(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "PrintedMaterial" in type:
                    object = PrintedMaterial(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Herbarium" in type:
                    object = Herbarium(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Specimen" in type:
                    object = Specimen(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Painting" in type:
                    object = Painting(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Model" in type:
                    object = Model(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Map" in type:
                    object = Map(id=row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
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
    
    def getAllCulturalHeritageObjects(self):
        result = []
        for query in self.metadataQuery:
            df = query.getAllCulturalHeritageObjects()

            for _, row in df.iterrows():
                type = row['type']
                if "NauticalChart" in type:
                    object = NauticalChart(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    result.append(object)

                elif "ManuscriptPlate" in type:
                    object = ManuscriptPlate(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])    
                    result.append(object) 

                elif "ManuscriptVolume" in type:
                    object = ManuscriptVolume(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    result.append(object)

                elif "Book" in type:
                    object = PrintedVolume(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    result.append(object)

                elif "PrintedMaterial" in type:
                    object = PrintedMaterial(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    result.append(object)

                elif "Herbarium" in type:
                    object = Herbarium(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    result.append(object)

                elif "Specimen" in type:
                    object = Specimen(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    result.append(object)

                elif "Painting" in type:
                    object = Painting(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    result.append(object)

                elif "Model" in type:
                    object = Model(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    result.append(object)

                elif "Map" in type:
                    object = Map(id=row["id"],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    result.append(object)
        return result
    
class AdvancedMashup(BasicMashup):
    
    def getActivitiesOnObjectsAuthoredBy(self, personId):
        cultural_objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        id_list = []
        for object in cultural_objects:
            id = object.id
            id_list.append(id)   
        activities = self.getAllActivities()
        result_list = []
        for activity in activities:
            object_id = activity.refers_to
            for id in id_list:
                object_reference = "object-"+str(id-1)
                if object_id == object_reference:
                    result_list.append(activity) 
        return result_list    

    def getObjectsHandledByResponsibleInstitution(self, institution):
        activities = self.getActivitiesByResponsibleInstitution(institution)
    # get ids of object in activity
        id_list = []
        for activity in activities:
            object_id = activity.refers_to
            id_list.append(object_id)
        #print(id_list)
    
        cultural_objects = self.getAllCulturalHeritageObjects()
    # Crea un dizionario per mappare gli ID degli oggetti 
        #cultural_object_dict = {obj.id: obj for obj in cultural_objects}
    # Recupera gli oggetti del patrimonio culturale utilizzando gli ID estratti
        result = []
        for obj in cultural_objects:
            if obj.id in id_list:
                result.append(obj)

        return result 

    def getObjectsHandledByResponsiblePerson(self, name):
        activities = self.getActivitiesByResponsiblePerson(name)
        id_list = []
        for activity in activities:
            object_id = activity.refers_to
            id_list.append(object_id)
        cultural_objects = self.getAllCulturalHeritageObjects()
        result = []
        for obj in cultural_objects:
            if obj.id in id_list:
                result.append(obj)

        return result
                    

data = ProcessDataUploadHandler()
data.setDbPathOrUrl("database.db")
data.pushDataToDb("process.json")

process_query_handler = ProcessDataQueryHandler()
process_query_handler.setDbPathOrUrl("database.db")

data2 = MetadataUploadHandler()
data2.setDbPathOrUrl("http://192.168.1.169:9999/blazegraph/sparql")
data2.pushDataToDb("meta.csv")

metadata_query_handler = MetadataQueryHandler()
metadata_query_handler.setDbPathOrUrl("http://192.168.1.169:9999/blazegraph/sparql") 

advanced_mashup = AdvancedMashup()
advanced_mashup.addMetadataHandler(metadata_query_handler)
advanced_mashup.addProcessHandler(process_query_handler)

resultlist = advanced_mashup.getObjectsHandledByResponsibleInstitution("Council")
pprint(resultlist)