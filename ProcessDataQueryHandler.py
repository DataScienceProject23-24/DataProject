from sqlite3 import connect
import pandas as pd
import pprint

class Handler(object):
    def __init__(self):
        self.dbPathOrUrl = ""
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    def setDbPathOrUrl(self,pathOrUrl:str):
        self.dbPathOrUrl = pathOrUrl
        return True

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass    
    
class ProcessDataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()
    def pushDataToDb(self, path):
        process_json = pd.read_json(path)
        object_ids=process_json[["object id"]]
        object_internal_id = []
        for idx, row in object_ids.iterrows():
            object_internal_id.append("object-" + str(idx+1))
        object_ids.insert(0, "objectId", pd.Series(object_internal_id, dtype="string"))
        
        norm_df=[]
        cols = ["acquisition","processing","modelling","optimising","exporting"]
        for col in cols:
            norm = pd.json_normalize(process_json[col])
            norm["tool"] = norm["tool"].astype("str")
            norm.insert((norm.shape[1]),"objectId",object_ids["objectId"])
            norm_df.append(norm)
        
        with connect(self.getDbPathOrUrl()) as con:
            norm_df[0].to_sql("Acquisition", con, if_exists="replace", index=False)
            norm_df[1].to_sql("Processing", con, if_exists="replace", index=False)
            norm_df[2].to_sql("Modelling", con, if_exists="replace", index=False)
            norm_df[3].to_sql("Optimizing", con, if_exists="replace", index=False)
            norm_df[4].to_sql("Exporting", con, if_exists="replace", index=False)
        
        return True    
   
class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
    def getAllActivities(self):
        with connect(self.getDbPathOrUrl()) as con:
            query = "SELECT * FROM *"
            df_sql = pd.read_sql(query, con)
            return df_sql
    
    def getActivitiesByResponsibleInstitutions(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            query = f"SELECT * FROM  WHERE institute={partialName};"

    def getActivitiesByResponsiblePerson(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            query = f"SELECT * FROM  WHERE person={partialName};"
    
    def getActivitiesUsingTool(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            query = f"SELECT * FROM  WHERE tool={partialName};"

    def getActivitiesStartedAfter(self, date):
        with connect(self.getDbPathOrUrl()) as con:
            query = f"SELECT * FROM  WHERE start>={date};"

    def getActivitiesEndedBefore(self, date):
        with connect(self.getDbPathOrUrl()) as con:
            query = f"SELECT * FROM  WHERE end<={date};"
    
    def getAcquisitionsByTechnique(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            query = f"SELECT * FROM acquisition WHERE technique={partialName};"        


path = Handler()
path.setDbPathOrUrl("")

data = ProcessDataUploadHandler()
data.pushDataToDb("process.json")

query_handler = ProcessDataQueryHandler()        

df_activities = query_handler.getAllActivities()

pprint(df_activities)

           