from rdflib import Graph, RDF, URIRef, Literal
import pandas as pd
from sqlite3 import connect
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
from pprint import pprint

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
        object_id = []
        for idx, row in object_ids.iterrows():
            object_id.append("object-" + str(idx))
        object_ids.insert(0, "objectId", pd.Series(object_id, dtype="string"))
        
        norm_df=[]
        activity_id = []
        activities_df = pd.DataFrame()
        cols = ["acquisition","processing","modelling","optimising","exporting"]
        for col in cols:
            norm = pd.json_normalize(process_json[col])
            norm["tool"] = norm["tool"].astype("str")
            norm.insert((norm.shape[1]),"objectId",object_ids["objectId"])
            internal_id = []
            for idx, row in norm.iterrows():
                internal_id.append(col + "-" + str(idx))
                activity_id.append(col + "-" + str(idx))
            norm.insert(loc=0, column='internalId', value=internal_id)
            norm_df.append(norm)

        #print("Normalized dataframes:", [df.head() for df in norm_df])  # Debug print

        activities_df["activityId"] = activity_id

        
        with connect(self.getDbPathOrUrl()) as con:
            activities_df.to_sql("Activities", con, if_exists="replace", index=False)
            norm_df[0].to_sql("Acquisition", con, if_exists="replace", index=False)
            norm_df[1].to_sql("Processing", con, if_exists="replace", index=False)
            norm_df[2].to_sql("Modelling", con, if_exists="replace", index=False)
            norm_df[3].to_sql("Optimising", con, if_exists="replace", index=False)
            norm_df[4].to_sql("Exporting", con, if_exists="replace", index=False)
        
        #print("Data pushed to database successfully")  # Debug print
        return True


class QueryHandler(Handler):
    def __init__(self):
        super().__init__()
    def getById(self, id: str):
        endpoint = self.getDbPathOrUrl()
        query = """
        PREFIX res: <https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?type ?title ?date ?owner ?place ?authorName
        WHERE
            {
            SELECT* WHERE{
            ?id schema:identifier '%i'.  
            VALUES ?type {res:NauticalChart res:ManuscriptPlate schema:Manuscript schema:Book res:PrintedMaterial res:Herbarium res:Specimen schema:Painting res:Model schema:Map}
            ?id rdf:type ?type.
            ?id schema:title ?title.
            ?id schema:dateCreated ?date.
            ?id schema:acquiredFrom ?owner.
            ?id schema:location ?place.
            OPTIONAL { SELECT * WHERE {
              ?authorId schema:name ?authorName.
              ?id schema:author ?authorId.}
              }
            }
        }
        """%(id)
        df_entity = get(endpoint, query, True)
        return df_entity

class ProcessDataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
  
    def getAllActivities(self):
        with connect(self.getDbPathOrUrl()) as con:
            q1 = "SELECT * FROM Acquisition"
            df_a = pd.read_sql(q1, con)
            q2 = "SELECT * FROM Processing"
            df_p = pd.read_sql(q2,con)
            q3 = "SELECT * FROM Modelling"
            df_m = pd.read_sql(q3,con)
            q4 = "SELECT * FROM Optimising"
            df_o = pd.read_sql(q4,con)
            q5 = "SELECT * FROM Exporting"
            df_e = pd.read_sql(q5,con)

            union_list = [df_a, df_p, df_m, df_o, df_e]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union

    def getActivitiesByResponsibleInstitution(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            q1 = 'SELECT * FROM Acquisition WHERE "responsible institute" LIKE ?;'
            df_a = pd.read_sql(q1, con, params=(f"%{partialName}%",))
            q2 = 'SELECT * FROM Processing WHERE "responsible institute" LIKE ?;'
            df_p = pd.read_sql(q2, con, params=(f"%{partialName}%",))
            q3 = 'SELECT * FROM Modelling WHERE "responsible institute" LIKE ?;'
            df_m = pd.read_sql(q3, con, params=(f"%{partialName}%",))
            q4 = 'SELECT * FROM Optimising WHERE "responsible institute" LIKE ?;'
            df_o = pd.read_sql(q4, con, params=(f"%{partialName}%",))
            q5 = 'SELECT * FROM Exporting WHERE "responsible institute" LIKE ?;'
            df_e = pd.read_sql(q5, con, params=(f"%{partialName}%",))

            union_list = [df_a, df_p, df_m, df_o, df_e]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union    
    
    def getActivitiesByResponsiblePerson(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            q1 = 'SELECT * FROM Acquisition WHERE "responsible person" LIKE ?;'
            df_a = pd.read_sql(q1, con, params=(f"%{partialName}%",))
            q2 = 'SELECT * FROM Processing WHERE "responsible person" LIKE ?;'
            df_p = pd.read_sql(q2, con, params=(f"%{partialName}%",))
            q3 = 'SELECT * FROM Modelling WHERE "responsible person" LIKE ?;'
            df_m = pd.read_sql(q3, con, params=(f"%{partialName}%",))
            q4 = 'SELECT * FROM Optimising WHERE "responsible person" LIKE ?;'
            df_o = pd.read_sql(q4, con, params=(f"%{partialName}%",))
            q5 = 'SELECT * FROM Exporting WHERE "responsible person" LIKE ?;'
            df_e = pd.read_sql(q5, con, params=(f"%{partialName}%",))

            union_list = [df_a, df_p, df_m, df_o, df_e]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union


    def getActivitiesUsingTool(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            q1 = 'SELECT * FROM Acquisition WHERE "tool" LIKE ?;'
            df_a = pd.read_sql(q1, con, params=(f"%{partialName}%",))
            q2 = 'SELECT * FROM Processing WHERE "tool" LIKE ?;'
            df_p = pd.read_sql(q2, con, params=(f"%{partialName}%",))
            q3 = 'SELECT * FROM Modelling WHERE "tool" LIKE ?;'
            df_m = pd.read_sql(q3, con, params=(f"%{partialName}%",))
            q4 = 'SELECT * FROM Optimising WHERE "tool" LIKE ?;'
            df_o = pd.read_sql(q4, con, params=(f"%{partialName}%",))
            q5 = 'SELECT * FROM Exporting WHERE "tool" LIKE ?;'
            df_e = pd.read_sql(q5, con, params=(f"%{partialName}%",))

            union_list = [df_a, df_p, df_m, df_o, df_e]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union
  
    def getActivitiesStartedAfter(self, date):
        with connect(self.getDbPathOrUrl()) as con:
            q1 = 'SELECT * FROM Acquisition WHERE "start date" >= ?;'
            df_a = pd.read_sql(q1, con, params=(date,))
            q2= 'SELECT * FROM Processing WHERE "start date" >= ?;'
            df_p = pd.read_sql(q2, con, params=(date,))
            q3 = 'SELECT * FROM Modelling WHERE "start date" >= ?;'
            df_m = pd.read_sql(q3,con,params=(date,))
            q4 = 'SELECT * FROM Optimising WHERE "start date" >= ?;'
            df_o = pd.read_sql(q4,con,params=(date,))
            q5 = 'SELECT * FROM Exporting WHERE "start date" >= ?;'
            df_e = pd.read_sql(q5,con, params=(date,))

            union_list = [df_a, df_p, df_m, df_o, df_e]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union


    def getActivitiesEndedBefore(self, date):
        with connect(self.getDbPathOrUrl()) as con:
            q1 = 'SELECT * FROM Acquisition WHERE "end date" <= ?;'
            df_a = pd.read_sql(q1, con, params=(date,))
            q2= 'SELECT * FROM Processing WHERE "end date" <= ?;'
            df_p = pd.read_sql(q2, con, params=(date,))
            q3 = 'SELECT * FROM Modelling WHERE "end date" <= ?;'
            df_m = pd.read_sql(q3,con, params=(date,))
            q4 = 'SELECT * FROM Optimising WHERE "end date" <= ?;'
            df_o = pd.read_sql(q4,con, params=(date,))
            q5 = 'SELECT * FROM Exporting WHERE "end date" <= ?;'
            df_e = pd.read_sql(q5,con, params=(date,))

            union_list = [df_a, df_p, df_m, df_o, df_e]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union

    
    def getAcquisitionsByTechnique(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:        
            q1 = 'SELECT * FROM Acquisition WHERE "technique" LIKE ?;'
            df_a = pd.read_sql(q1, con, params=(f"%{partialName}%",))
            q2= 'SELECT * FROM Processing WHERE "technique" LIKE ?;'
            df_p = pd.read_sql(q2, con, params=(f"%{partialName}%",))
            q3 = 'SELECT * FROM Modelling WHERE "technique" LIKE ?;'
            df_m = pd.read_sql(q3,con,params=(f"%{partialName}%",))
            q4 = 'SELECT * FROM Optimising WHERE "technique" LIKE ?;'
            df_o = pd.read_sql(q4,con, params=(f"%{partialName}%",))
            q5 = 'SELECT * FROM Exporting WHERE "technique" LIKE ?;'
            df_e = pd.read_sql(q5,con, params=(f"%{partialName}%",))

            union_list = [df_a, df_p, df_m, df_o, df_e]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union


data = ProcessDataUploadHandler()
data.setDbPathOrUrl("database.db")
data.pushDataToDb("process.json")

query_handler = ProcessDataQueryHandler()        
query_handler.setDbPathOrUrl("database.db")

df_activities = query_handler.getActivitiesByResponsibleInstitution("Philology")
#df_technique = query_handler.getAcquisitionsByTechnique("Photogrammetry")
#df_activities_ended_before = query_handler.getActivitiesEndedBefore("2023-04-21")
#df_activities_started_after = query_handler.getActivitiesStartedAfter("2023-04-21")


#pprint(df_activities.to_dict())
#pprint(df_technique.to_dict())
pprint(df_activities)         