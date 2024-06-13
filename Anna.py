import pandas as pd
from pandas import concat
from pprint import pprint
from Handler import MetadataUploadHandler, ProcessDataUploadHandler, MetadataQueryHandler, ProcessDataQueryHandler
from DataModelClasses import Person, CulturalHeritageObject, Activity, Acquisition, Processing, Modelling, Optimizing, Exporting, NauticalChart, ManuscriptPlate, ManuscriptVolume, PrintedVolume, PrintedMaterial, Herbarium, Specimen, Painting, Model, Map



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
                entry = Optimizing(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])    
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
                entry = Optimizing(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])    
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
                entry = Optimizing(row["responsible institute"],row["responsible person"],row["tool"],row["start date"],row["end date"],row["objectId"])    
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
                if author != "NaN":              #!!!!!!
                    object = Person(name = row['authorName'])
                    result.append(object)   
                else:
                    return(None)
        return result



    
u = MetadataUploadHandler()
endpoint = " http://192.168.194.161:9999/blazegraph/sparql" #
path = r"C:\Users\annap\Documents\GitHub\DataProject\resources\meta.csv"
u.setDbPathOrUrl(endpoint)
u.pushDataToDb(path)
q = MetadataQueryHandler()
q.setDbPathOrUrl(endpoint)
bm = BasicMashup()
bm.addMetadataHandler(q)

#print(bm.getAllPeople()) --> it works!
#print(bm.getAllCulturalHeritageObjects()) --> it works!!
#print(bm.getAuthorsOfCulturalHeritageObject("...")) --> it works!!!

# DON'T WORK
#print(bm.getEntityById("ULAN:500114874")) --> cannot access local variable 'df_entity' where it is not associated with a value
#print(bm.getEntityById('1')) --> NONE ???




