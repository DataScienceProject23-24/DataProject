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
                entry = Acquisition(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"],row["technique"])
            elif activity_type == "processing":
                entry = Processing(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])   
            elif activity_type == "modelling":
                entry = Modelling(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])
            elif activity_type == "optimizing":
                entry = Optimising(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])    
            elif activity_type == "exporting":
                entry = Exporting(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])
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
                entry = Acquisition(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"],row["technique"])
            elif activity_type == "processing":
                entry = Processing(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])   
            elif activity_type == "modelling":
                entry = Modelling(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])
            elif activity_type == "optimizing":
                entry = Optimising(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])    
            elif activity_type == "exporting":
                entry = Exporting(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])
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
                entry = Acquisition(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"],row["technique"])
            elif activity_type == "processing":
                entry = Processing(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])   
            elif activity_type == "modelling":
                entry = Modelling(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])
            elif activity_type == "optimizing":
                entry = Optimising(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])    
            elif activity_type == "exporting":
                entry = Exporting(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])
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
            entry = Acquisition(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"],row["technique"])
            list_acquisitions.append(entry)
        return list_acquisitions


    def getAllPeople(self):
        result = []
        for query in self.metadataQuery:
            df = query.getAllPeople()

            for _, row in df.iterrows():
                object = Person(name = row['authorName'])
                result.append(object)
        return result
    
    def getAllCulturalHeritageObjects(self):
        result = []
        for query in self.metadataQuery:
            df = query.getAllCulturalHeritageObjects()

            for _, row in df.iterrows():
                type = row['type']
                if "NauticalChart" in type:
                    object = NauticalChart(id = row['id'], title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "ManuscriptPlate" in type:
                    object = ManuscriptPlate(id = row['id'],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])    
                    result.append(object) 

                elif "ManuscriptVolume" in type:
                    object = ManuscriptVolume(id = row['id'],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "PrintedVolume" in type:
                    object = PrintedVolume(id = row['id'],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "PrintedMaterial" in type:
                    object = PrintedMaterial(id = row['id'],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Herbarium" in type:
                    object = Herbarium(id = row['id'],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Specimen" in type:
                    object = Specimen(id = row['id'],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Painting" in type:
                    object = Painting(id = row['id'],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Model" in type:
                    object = Model(id = row['id'],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)

                elif "Map" in type:
                    object = Map(id = row['id'],title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                    result.append(object)
        return result


    def getEntityById(self, id): 
        for query in self.metadataQuery:
            df = query.getById(id)

            if df.empty:
                continue

            for _,row in df.iterrows():
                if "authorName" in df.columns:
                    author = row['authorName']
                    if author != "NaN":
                        return Person(id=id, name = row['authorName'])
                               
                else:
                    type = row["type"]
                    authors = row['authorName'].split(";") if "authorName" in row and row['authorName'] else []

                    if "NauticalChart" in type:
                        return NauticalChart(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                        

                    elif "ManuscriptPlate" in type:
                        return ManuscriptPlate(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])    
                        

                    elif "ManuscriptVolume" in type:
                        return ManuscriptVolume(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                        

                    elif "PrintedVolume" in type:
                        return PrintedVolume(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                        

                    elif "PrintedMaterial" in type:
                        return PrintedMaterial(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                        

                    elif "Herbarium" in type:
                        return Herbarium(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                        

                    elif "Specimen" in type:
                        return  Specimen(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                        

                    elif "Painting" in type:
                        return Painting(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                        

                    elif "Model" in type:
                        return Model(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                        

                    elif "Map" in type:
                        return Map(id=id, title=row['title'], date=row['date'], owner=row['owner'], place=row['place'], authors=row['hasAuthor'])
                   
                  
    

    def getAuthorsOfCulturalHeritageObject(self, id):
        result = []
        for query in self.metadataQuery:
            df = query.getAuthorsOfCulturalHeritageObject(id)

            for _, row in df.iterrows():
                author = row['authorName']
                if author != "NaN":              
                    object = Person(name = row['authorName'])
                    result.append(object)   
                else:
                    return(None)
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











class AdvancedMashup(BasicMashup):
    def getObjectsHandledByResponsiblePerson(self, partialName):
        result = []
        activities = self.getActivitiesByResponsiblePerson(partialName)
        ids = []
        for activity in activities:
            object_id = activity.refers_to 
            ids.append(object_id)
            print(ids)

        objects = self.getAllCulturalHeritageObjects()
        for object in objects:
            o_id = object.id
            #for o_id in ids:

            







         



    


    
u = MetadataUploadHandler()
endpoint = "http://10.201.14.57:9999/blazegraph/sparql" 
path = r"C:\Users\annap\Documents\GitHub\DataProject\resources\meta.csv"
u.setDbPathOrUrl(endpoint)
u.pushDataToDb(path)
q = MetadataQueryHandler()
q.setDbPathOrUrl(endpoint)
p = ProcessDataQueryHandler()
p.setDbPathOrUrl("database.db")
bm = BasicMashup()
am = AdvancedMashup()

bm.addMetadataHandler(q)
am.addMetadataHandler(q)
am.addProcessHandler


#print(bm.getAllPeople()) #--> it works!
#print(bm.getAllCulturalHeritageObjects()) --> it works!!
#print(bm.getAuthorsOfCulturalHeritageObject("1")) --> it works!!!

# DON'T WORK
#print(bm.getEntityById("ULAN:500114874")) #--> cannot access local variable 'df_entity' where it is not associated with a value
#print(bm.getEntityById('1')) #--> NONE ???


print(am.getObjectsHandledByResponsiblePerson("Alice Liddell"))

