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
    
    def combineAuthorsOfObjects(self,df,handler):
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

    def getEntityById(self, id):                                #checked for both person and obj ids 
        handler_list = self.metadataQuery
        df_list = []

        for handler in handler_list:
            entity = handler.getById(id)
            entity_update = self.combineAuthorsOfObjects(entity,handler)
            df_list.append(entity_update) #list of df
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("") #one big df

        for _,row in df_union.iterrows():
            if "authorName" in row:
                author = row['authorName']
                if author != "":
                    return Person(id=id,name = row['authorName'])
                elif author == "":
                    return None
                            
            else:
                type = row["type"]

                if "NauticalChart" in type:
                    return NauticalChart(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "ManuscriptPlate" in type:
                    return ManuscriptPlate(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])    
                    

                elif "ManuscriptVolume" in type:
                    return ManuscriptVolume(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Book" in type:
                    return PrintedVolume(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "PrintedMaterial" in type:
                    return PrintedMaterial(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Herbarium" in type:
                    return Herbarium(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Specimen" in type:
                    return  Specimen(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Painting" in type:
                    return Painting(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Model" in type:
                    return Model(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                    

                elif "Map" in type:
                    return Map(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['Authors'])
                
                else:
                    return None
            
    def getAllPeople(self):                     #checked!          
        handler_list = self.metadataQuery
        df_list = []
        result = []

        for handler in handler_list:
            df_list.append(handler.getAllPeople()) #list of df
        df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("") #one big df

        for _, row in df_union.iterrows():
            object = Person(id=row["authorId"],name = row['authorName'])
            result.append(object)
        return result
    

    def getAllCulturalHeritageObjects(self):        #checked, error with Authors!!!
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
    


    def getAuthorsOfCulturalHeritageObject(self, id):           #checked! 
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
    

    def getCulturalHeritageObjectsAuthoredBy(self, authorId):       #checked, error on Authors!!
        result = []
        handler_list = self.metadataQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getCulturalHeritageObjectsAuthoredBy(authorId)) #list of df
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
    
    def getAllActivities(self):     #checked!
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getAllActivities()) #list of df
            df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")
            
            for _, row in df_union.iterrows():
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
    
    def getActivitiesByResponsibleInstitution(self, partialName):       #checked, empty list???????????
        result = []
        handler_list = self.processQuery
        df_list = []

        for handler in handler_list:
            df_list.append(handler.getActivitiesByResponsibleInstitution(partialName)) #list of df
            df_union = pd.concat(df_list, ignore_index=True).drop_duplicates().fillna("")

            for _, row in df_union.iterrows():
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
    
    def getActivitiesByResponsiblePerson(self, person):     #checked!
        
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
    
    
    def getActivitiesUsingTool(self, partialName:str):          #checked!
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
    
    def getActivitiesStartedAfter(self, date:str):      #checked!
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

    def getActivitiesEndedBefore(self, date:str):       #checked, but it also retrieve activities without data
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
    
    def getAcquisitionsByTechnique(self, partialName:str):          #checked! 
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
    def getActivitiesOnObjectsAuthoredBy(self, personId):               #checked, special case for acquisition to retrieve also the technique ()
        cultural_objects = self.getCulturalHeritageObjectsAuthoredBy(personId)
        id_list = []
        for object in cultural_objects:
            id_list.append(object.id)   
        activities = self.getAllActivities()
        result_list = []
        for activity in activities:
            if activity.refers_to in id_list:
                result_list.append(activity) 
        return result_list       


    def getObjectsHandledByResponsiblePerson(self, name):              #checked!
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


    def getObjectsHandledByResponsibleInstitution(self, institution):       #checked, empty list as getActivitiesByResponsibleInstitution method
        activities = self.getActivitiesByResponsibleInstitution(institution)
        id_list = []
        for activity in activities:
            object_id = activity.refers_to
            id_list.append(object_id)
        print(id_list)
    
        cultural_objects = self.getAllCulturalHeritageObjects()
        result = []
        for obj in cultural_objects:
            if obj.id in id_list:
                result.append(obj)
                #print(result)
        return result
    
    def getAuthorsOfObjectsAcquiredInTimeFrame(self, start:str, end:str):                     #not working, empty list 
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

data = ProcessDataUploadHandler()
data.setDbPathOrUrl("activities.db")
data.pushDataToDb("process.json")

process_query_handler = ProcessDataQueryHandler()
process_query_handler.setDbPathOrUrl("activities.db")

data2 = MetadataUploadHandler()
data2.setDbPathOrUrl("http://192.168.1.169:9999/blazegraph/sparql")
data2.pushDataToDb("meta.csv")

metadata_query_handler = MetadataQueryHandler()
metadata_query_handler.setDbPathOrUrl("http://192.168.1.169:9999/blazegraph/sparql")

mashup = BasicMashup()
mashup.addProcessHandler(process_query_handler)
mashup.addMetadataHandler(metadata_query_handler)

r = mashup.getCulturalHeritageObjectsAuthoredBy("VIAF:100190422")
print(r)
#print(len(r))
for i in r:
    print(i.id, i.title, i.date, i.owner, i.place)
    authors = []
    authors.append(i.hasAuthor)
for a in authors:
    print(a.id, a.name)        

#authors = row.get('authors', '')  # Ensure you get the 'authors' from the row