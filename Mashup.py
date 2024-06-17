import pandas as pd
from pandas import concat
from pprint import pprint
from Handler import MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from DataModelClasses import Person, CulturalHeritageObject, Activity, Acquisition, Processing, Modelling, Optimising, Exporting, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map

class BasicMashup(object):
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
    
    def getEntityById(self, id): 
        handler_list = self.metadataQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getById(id)) #list of df
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("") #one big df

        for _,row in df_union.iterrows():
            if "authorName" in row:
                author = row['authorName']
                if author != "":
                    return Person(id=id,name = row['authorName'])
                            
            else:
                type = row["type"]
                authors_list = row['Authors'].split(";") if "Authors" in row and row['Authors'] else []

                if "NauticalChart" in type:
                    return NauticalChart(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)
                    

                elif "ManuscriptPlate" in type:
                    return ManuscriptPlate(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)    
                    

                elif "ManuscriptVolume" in type:
                    return ManuscriptVolume(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)
                    

                elif "Book" in type:
                    return PrintedVolume(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)
                    

                elif "PrintedMaterial" in type:
                    return PrintedMaterial(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)
                    

                elif "Herbarium" in type:
                    return Herbarium(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)
                    

                elif "Specimen" in type:
                    return  Specimen(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)
                    

                elif "Painting" in type:
                    return Painting(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)
                    

                elif "Model" in type:
                    return Model(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)
                    

                elif "Map" in type:
                    return Map(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=authors_list)
                
                else:
                    return None
            
    def getAllPeople(self):
        handler_list = self.metadataQuery
        df_list = []
        result = []

        for handler in handler_list:
            df_list.append(handler.getAllPeople(id)) #list of df
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("") #one big df

        for _, row in df_union.iterrows():
            object = Person(id=row["authorId"],name = row['authorName'])
            result.append(object)
        return result
    

    def getAllCulturalHeritageObjects(self):
        handler_list = self.metadataQuery
        df_list = []
        result = []

        for handler in handler_list:
            df_list.append(handler.getAllCulturalHeritageObjects()) #list of df
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
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
    


    def getAuthorsOfCulturalHeritageObject(self, id):
        result = []
        handler_list = self.metadataQuery
        df_list = []
        

        for handler in handler_list:
            df_list.append(handler.getAuthorsOfCulturalHeritageObject(id)) #list of df
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            author = row['authorName']
            if author != "":             
                object = Person(id=row["authorId"],name = row['authorName'])
                result.append(object)   
            else:
                return(None)
        return result
    

    def getCulturalHeritageObjectsAuthoredBy(self, authorId):

        result = []

        for handler in self.metadataQuery:
            df = handler.getCulturalHeritageObjectsAuthoredBy(authorId) 

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
    
    
    def getActivitiesUsingTool(self, partialName:str):
        handler_list = self.processQuery
        df_list = []
        for handler in handler_list:
            df_list.append(handler.getActivitiesUsingTool(partialName))
        df_union = pd.concat(df_list, ignore_index=True)
        list_activities = []
        for idx, row in df_union.iterrows():
            activity_type = (row["internalId"].split("-"))[0]
            if activity_type == "acquisition":
                entry = Acquisition(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"],technique=row["technique"])
            elif activity_type == "processing":
                entry = Processing(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])   
            elif activity_type == "modelling":
                entry = Modelling(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])
            elif activity_type == "optimising":
                entry = Optimising(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])    
            elif activity_type == "exporting":
                entry = Exporting(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])
            list_activities.append(entry)
        return list_activities
    
    def getActivitiesStartedAfter(self, date:str):
        handler_list = self.processQuery
        df_list = []
        for handler in handler_list:
            df_list.append(handler.getActivitiesStartedAfter(date))
        df_union = pd.concat(df_list, ignore_index=True)
        list_activities = []
        for idx, row in df_union.iterrows():
            activity_type = (row["internalId"].split("-"))[0]
            if activity_type == "acquisition":
                entry = Acquisition(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"],technique=row["technique"])
            elif activity_type == "processing":
                entry = Processing(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])   
            elif activity_type == "modelling":
                entry = Modelling(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])
            elif activity_type == "optimising":
                entry = Optimising(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])    
            elif activity_type == "exporting":
                entry = Exporting(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])
            list_activities.append(entry) 
        return list_activities

    def getActivitiesEndedBefore(self, date:str):
        handler_list = self.processQuery
        df_list = []
        for handler in handler_list:
            df_list.append(handler.getActivitiesEndedBefore(date))
        df_union = pd.concat(df_list, ignore_index=True)
        list_activities = []
        for idx, row in df_union.iterrows():
            activity_type = (row["internalId"].split("-"))[0]
            if activity_type == "acquisition":
                entry = Acquisition(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"],technique=row["technique"])
            elif activity_type == "processing":
                entry = Processing(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])   
            elif activity_type == "modelling":
                entry = Modelling(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])
            elif activity_type == "optimising":
                entry = Optimising(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])    
            elif activity_type == "exporting":
                entry = Exporting(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"])
            list_activities.append(entry)
        return list_activities
    
    def getAcquisitionsByTechnique(self, partialName:str):
        handler_list = self.processQuery
        df_list = []
        for handler in handler_list:
            df_list.append(handler.getAcquisitionsByTechnique(partialName))
        df_union = pd.concat(df_list, ignore_index=True)
        list_acquisitions = []
        for idx, row in df_union.iterrows():
            entry = Acquisition(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=row["objectId"],technique=row["technique"])
            list_acquisitions.append(entry)
        return list_acquisitions
    

class AdvancedMashup(BasicMashup):
    def __init__(self):
        super().__init__()
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
                if object_id == id:
                    result_list.append(activity) 
        return result_list       


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


    def getObjectsHandledByResponsibleInstitution(self, institution):
        activities = self.getActivitiesByResponsibleInstitution(institution)
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
    
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start:str, end:str):
        acquisition_start = [i.refers_to for i in self.getActivitiesStartedAfter(start) if type(i) is Acquisition]
        acquisition_end = [i.refers_to for i in self.getActivitiesEndedBefore(end) if type(i) is Acquisition]
        acquisition_list = [obj for obj in acquisition_start if obj in acquisition_end]
        authors_of_obj = set()
        for i in acquisition_list:
            authors = self.getAuthorsOfCulturalHeritageObject(str(i))
            for auth in authors:
                if auth is not None:
                    authors_of_obj.add((auth.id,auth.name))
        authors = [Person(id = auth[0],name=auth[1]) for auth in authors_of_obj]
        return authors
    


