import pandas as pd
from pandas import concat
from pprint import pprint
from Handler import MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from DataModelClasses import Person, CulturalHeritageObject, Activity, Acquisition, Processing, Modelling, Optimising, Exporting, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map

class BasicMashup(object):
    # V I R G I #
    def __init__(self):
        self.metadataQuery = []  
        self.processQuery = [] 

    def cleanMetadataHandlers(self):
        self.metadataQuery = [] 
        return True
    
    def cleanProcessHandlers(self):
        self.processQuery = [] 
        return True
    
    def addMetadataHandler(self, metadataHandler): 
        self.metadataQuery.append(metadataHandler)   
        return True
    
    def addProcessHandler(self, processHandler):
        self.processQuery.append(processHandler)
        return True 

    # E Z G I #
    def combineAuthorsOfObjects(self,df,handler):
        if "Authors" in df.columns:
            for idx, row in df.iterrows():
                if row["Authors"] != "":
                    id = row["id"]
                    authors_df = handler.getAuthorsOfCulturalHeritageObject(id)
                    authors_df.insert(loc=0,column="id",value=str(id))
                    authors_df.insert(loc=0,column="auth",value=authors_df['authorName'].astype(str) +"-"+ authors_df["authorId"].astype(str))
                    if authors_df.shape[0]>1:
                        output = authors_df.groupby('id')['auth'].apply(';'.join)
                        df.at[idx,"Authors"] = str(output.iloc[0])
                    else:
                        df.at[idx,"Authors"] = authors_df.iloc[0,0]
        return df.drop_duplicates()

    # A N N A #
    def getEntityById(self, id:str):                                 
        handler_list = self.metadataQuery
        df_list = []
        for handler in handler_list:
            entity = handler.getById(id)
            entity_update = self.combineAuthorsOfObjects(entity,handler)
            df_list.append(entity_update)
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("") 

        for _,row in df_union.iterrows():
            if "authorName" in row:
                author = row['authorName']
                if author != "":
                    return Person(id=str(id),name = row['authorName'])
                elif author == "":
                    return None
                            
            else:
                obj_type = row["type"]

                if "NauticalChart" in obj_type:
                    return NauticalChart(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "ManuscriptPlate" in obj_type:
                    return ManuscriptPlate(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])    
                    

                elif "ManuscriptVolume" in obj_type:
                    return ManuscriptVolume(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Book" in obj_type:
                    return PrintedVolume(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "PrintedMaterial" in obj_type:
                    return PrintedMaterial(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Herbarium" in obj_type:
                    return Herbarium(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Specimen" in obj_type:
                    return  Specimen(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Painting" in obj_type:
                    return Painting(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Model" in obj_type:
                    return Model(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Map" in obj_type:
                    return Map(id=str(id), title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                
                else:
                    return None
            
    def getAllPeople(self):                              
        handler_list = self.metadataQuery
        df_list = []
        result = []

        for handler in handler_list:
            df_list.append(handler.getAllPeople()) 
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("") 

        for _, row in df_union.iterrows():
            object = Person(id=str(row["authorId"]),name = row['authorName'])
            result.append(object)
        return result
    

    def getAllCulturalHeritageObjects(self):        
        handler_list = self.metadataQuery
        df_list = []
        result = []

        for handler in handler_list:
            df_objects = handler.getAllCulturalHeritageObjects()
            df_object_update = self.combineAuthorsOfObjects(df_objects,handler)
            df_list.append(df_object_update) #list of df
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            obj_type = row['type']
            if "NauticalChart" in obj_type:
                object = NauticalChart(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "ManuscriptPlate" in obj_type:
                object = ManuscriptPlate(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])    
                result.append(object) 

            elif "ManuscriptVolume" in obj_type:
                object = ManuscriptVolume(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Book" in obj_type:
                object = PrintedVolume(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "PrintedMaterial" in obj_type:
                object = PrintedMaterial(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Herbarium" in obj_type:
                object = Herbarium(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Specimen" in obj_type:
                object = Specimen(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Painting" in obj_type:
                object = Painting(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Model" in obj_type:
                object = Model(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Map" in obj_type:
                object = Map(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)
        return result
    


    def getAuthorsOfCulturalHeritageObject(self, id:str):            
        result = []
        handler_list = self.metadataQuery
        df_list = []
        
        for handler in handler_list:
            df_list.append(handler.getAuthorsOfCulturalHeritageObject(id)) 
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            author = row['authorName']
            if author != "":             
                object = Person(id=str(row["authorId"]),name = row['authorName'])
                result.append(object)   
            else:
                return(None)
        return result
    
    # V A L E #
    def getCulturalHeritageObjectsAuthoredBy(self, authorId:str):       
        result = []
        handler_list = self.metadataQuery
        df_list = []

        for handler in handler_list:
            df_objects = handler.getCulturalHeritageObjectsAuthoredBy(authorId)
            df_object_update = self.combineAuthorsOfObjects(df_objects,handler)
            df_list.append(df_object_update) 
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():

            obj_type = row['type']

            if "NauticalChart" in obj_type:
                object = NauticalChart(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "ManuscriptPlate" in obj_type:
                object = ManuscriptPlate(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])    
                result.append(object) 

            elif "ManuscriptVolume" in obj_type:
                object = ManuscriptVolume(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Book" in obj_type:
                object = PrintedVolume(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "PrintedMaterial" in obj_type:
                object = PrintedMaterial(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Herbarium" in obj_type:
                object = Herbarium(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Specimen" in obj_type:
                object = Specimen(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Painting" in obj_type:
                object = Painting(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Model" in obj_type:
                object = Model(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)

            elif "Map" in obj_type:
                object = Map(id = str(row["id"]),title=row['title'], date=str(row['date']), owner=row['owner'], place=row['place'], authors=row['Authors'])
                result.append(object)    

        return result
    
    def getAllActivities(self):     
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities()) 
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
        
        for _, row in df_union.iterrows():
            type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if type == "acquisition":
                object = Acquisition(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to, technique=row['technique'])
                result.append(object)

            elif type == "processing":
                object = Processing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)
            
            elif type == "modelling":
                object = Modelling(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)
            
            elif type == "optimising":
                object = Optimising(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)

            elif type == "exporting":
                object = Exporting(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)    
                
        return result 
    
    def getActivitiesByResponsibleInstitution(self, partialName:str):       
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsibleInstitution(partialName)) 
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])
            
            if type == "acquisition":
                object = Acquisition(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to, technique=row['technique'])
                result.append(object)

            elif type == "processing":
                object = Processing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)
            
            elif type == "modelling":
                object = Modelling(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)
            elif type == "optimising":
                object = Optimising(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)

            elif type == "exporting":
                object = Exporting(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)  
        return result
    
    def getActivitiesByResponsiblePerson(self, partialName:str):     
        result = []
        handler_list = self.processQuery
        df_list = []
        
        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsiblePerson(partialName))
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

        for _, row in df_union.iterrows():
            type, id = row["internalId"].split("-")
            obj_refers_to = self.getEntityById(row["objectId"])

            if type == "acquisition":
                object = Acquisition(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to, technique=row['technique'])
                result.append(object)

            elif type == "processing":
                object = Processing(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)
            
            elif type == "modelling":
                object = Modelling(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)
            
            elif type == "optimising":
                object = Optimising(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)

            elif type == "exporting":
                object = Exporting(institute=row['responsible institute'], person=row['responsible person'], tools=row['tool'], start=row['start date'], end=row['end date'], refers_to=obj_refers_to)
                result.append(object)    
            
        return result
    
    # E Z G I #
    def getActivitiesUsingTool(self, partialName:str):          
        handler_list = self.processQuery
        df_list = []
        for handler in handler_list:
            df_list.append(handler.getActivitiesUsingTool(partialName))
        df_union = pd.concat(df_list, ignore_index=True)
        list_activities = []
        for idx, row in df_union.iterrows():
            activity_type = (row["internalId"].split("-"))[0]
            obj_refers_to = self.getEntityById(row["objectId"])
            if activity_type == "acquisition":
                entry = Acquisition(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to,technique=row["technique"])
            elif activity_type == "processing":
                entry = Processing(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)   
            elif activity_type == "modelling":
                entry = Modelling(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)
            elif activity_type == "optimising":
                entry = Optimising(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)    
            elif activity_type == "exporting":
                entry = Exporting(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)
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
            obj_refers_to = self.getEntityById(row["objectId"])
            if activity_type == "acquisition":
                entry = Acquisition(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to,technique=row["technique"])
            elif activity_type == "processing":
                entry = Processing(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)   
            elif activity_type == "modelling":
                entry = Modelling(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)
            elif activity_type == "optimising":
                entry = Optimising(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)    
            elif activity_type == "exporting":
                entry = Exporting(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)
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
            obj_refers_to = self.getEntityById(row["objectId"])
            if activity_type == "acquisition":
                entry = Acquisition(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to,technique=row["technique"])
            elif activity_type == "processing":
                entry = Processing(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)   
            elif activity_type == "modelling":
                entry = Modelling(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)
            elif activity_type == "optimising":
                entry = Optimising(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)    
            elif activity_type == "exporting":
                entry = Exporting(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to)
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
            obj_refers_to = self.getEntityById(row["objectId"])
            entry = Acquisition(institute=row["responsible institute"],person=row["responsible person"],tools=row["tool"],start=row["start date"],end=row["end date"],refers_to=obj_refers_to,technique=row["technique"])
            list_acquisitions.append(entry)
        return list_acquisitions
    

class AdvancedMashup(BasicMashup): 
    def __init__(self):
        super().__init__()

    # V A L E #
    def getActivitiesOnObjectsAuthoredBy(self, personId:str):               
        cultural_objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        id_list = []
        for object in cultural_objects:
            id_list.append(object.id)   
        activities = self.getAllActivities()
        result_list = []
        for activity in activities:
            if (activity.refersTo()).id in id_list:
                result_list.append(activity) 
        return result_list       


    # V I R G I #
    def getObjectsHandledByResponsiblePerson(self, name:str):              
        activities = self.getActivitiesByResponsiblePerson(name)
        id_list = []
        for activity in activities:
            object_id = (activity.refersTo()).id
            id_list.append(object_id)

        cultural_objects = self.getAllCulturalHeritageObjects()
        result = []

        for obj in cultural_objects:
            if obj.id in id_list:
                result.append(obj)

        return result


    # A N N A #
    def getObjectsHandledByResponsibleInstitution(self, institution:str):       
        activities = self.getActivitiesByResponsibleInstitution(institution)
        id_list = []
        for activity in activities:
            object_id = (activity.refersTo()).id
            id_list.append(object_id)
    
        cultural_objects = self.getAllCulturalHeritageObjects()
        result = []
        for obj in cultural_objects:
            if obj.id in id_list:
                result.append(obj)
        return result

    # E Z G I #
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start:str, end:str):                     
        acquisition_start = [(i.refersTo()).id for i in self.getActivitiesStartedAfter(start) if type(i) is Acquisition]
        acquisition_end = [(i.refersTo()).id for i in self.getActivitiesEndedBefore(end) if type(i) is Acquisition]
        acquisition_list = [obj for obj in acquisition_start if obj in acquisition_end]
        authors_of_obj = set()
        for i in acquisition_list:
            authors = self.getAuthorsOfCulturalHeritageObject(str(i))
            for auth in authors:
                if auth is not None:
                    authors_of_obj.add((auth.id,auth.name))
        authors = [Person(id = str(auth[0]),name=auth[1]) for auth in authors_of_obj]
        return authors

