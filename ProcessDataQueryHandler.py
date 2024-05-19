from rdflib import Graph, RDF, URIRef, Literal
import pandas as pd
from sqlite3 import connect
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

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
        
        activities_df["activityId"] = activity_id

        
        with connect(self.getDbPathOrUrl()) as con:
            activities_df.to_sql("Activities", con, if_exists="replace", index=False)
            norm_df[0].to_sql("Acquisition", con, if_exists="replace", index=False)
            norm_df[1].to_sql("Processing", con, if_exists="replace", index=False)
            norm_df[2].to_sql("Modelling", con, if_exists="replace", index=False)
            norm_df[3].to_sql("Optimizing", con, if_exists="replace", index=False)
            norm_df[4].to_sql("Exporting", con, if_exists="replace", index=False)

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
            q4 = "SELECT * FROM Optimizing"
            df_o = pd.read_sql(q4,con)
            q5 = "SELECT * FROM Exporting"
            df_e = pd.read_sql(q5,con)

            union_list = [df_a, df_p, df_m, df_o, df_e]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union
    
    def getActivitiesByResponsibleInstitutions(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            cursor = con.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            union_list = []
            for table in tables:
                table_name = table[0]
                query = f'SELECT "responsible institute" FROM {table_name} WHERE "responsible institute" LIKE ?;'
                df = pd.read_sql(query, con, params=(f"%{partialName}%",))
                union_list.append(df)
            
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union

    def getActivitiesByResponsiblePerson(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            cursor = con.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            union_list = []
            for table in tables:
                table_name = table[0]
                query = f'SELECT "responsible person" FROM {table_name} WHERE "responsible person" LIKE ?;'
                df = pd.read_sql(query, con, params=(f"%{partialName}%",))
                union_list.append(df)
            
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union  
    
    def getActivitiesUsingTool(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:
            cursor = con.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            union_list = []
            for table in tables:
                table_name = table[0]
                query = f'SELECT tool FROM {table_name};'
                df = pd.read_sql(query, con)
                df_result = df[df['tool'].apply(lambda tools: any(partialName in tool for tool in tools))]
                union_list.append(df_result)
            
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union

    def getActivitiesStartedAfter(self, date):
        with connect(self.getDbPathOrUrl()) as con:
            q1 = f"SELECT * FROM Acquisition WHERE start>= '{date}';"
            df_ac = pd.read_sql(q1, con)
            q2= f"SELECT * FROM Processing WHERE start>= '{date}';"
            df_pr = pd.read_sql(q2, con)
            q3 = f"SELECT * FROM Modelling WHERE start>= '{date}';"
            df_mod = pd.read_sql(q3,con)
            q4 = f"SELECT * FROM Optimizing WHERE start>= '{date}';"
            df_opt = pd.read_sql(q4,con)
            q5 = f"SELECT * FROM Exporting WHERE start>= '{date}';"
            df_ex = pd.read_sql(q5,con)

            union_list = [df_ac, df_pr, df_mod, df_opt, df_ex]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union


    def getActivitiesEndedBefore(self, date):
        with connect(self.getDbPathOrUrl()) as con:
            q1 = f"SELECT * FROM Acquisition WHERE end<= '{date}';"
            df_ac = pd.read_sql(q1, con)
            q2= f"SELECT * FROM Processing WHERE end<= '{date}';"
            df_pr = pd.read_sql(q2, con)
            q3 = f"SELECT * FROM Modelling WHERE end<= '{date}';"
            df_mod = pd.read_sql(q3,con)
            q4 = f"SELECT * FROM Optimizing WHERE end<= '{date}';"
            df_opt = pd.read_sql(q4,con)
            q5 = f"SELECT * FROM Exporting WHERE end<= '{date}';"
            df_ex = pd.read_sql(q5,con)

            union_list = [df_ac, df_pr, df_mod, df_opt, df_ex]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union
    
    def getAcquisitionsByTechnique(self, partialName):
        with connect(self.getDbPathOrUrl()) as con:        
            q1 = f"SELECT * FROM Acquisition WHERE technique= '%{partialName}';"
            df_ac = pd.read_sql(q1, con)
            q2= f"SELECT * FROM Processing WHERE technique= '%{partialName}';"
            df_pr = pd.read_sql(q2, con)
            q3 = f"SELECT * FROM Modelling WHERE technique= '%{partialName}';"
            df_mod = pd.read_sql(q3,con)
            q4 = f"SELECT * FROM Optimizing WHERE technique= '%{partialName}';"
            df_opt = pd.read_sql(q4,con)
            q5 = f"SELECT * FROM Exporting WHERE technique= '%{partialName}';"
            df_ex = pd.read_sql(q5,con)

            union_list = [df_ac, df_pr, df_mod, df_opt, df_ex]
            df_union = pd.concat(union_list, ignore_index=True)
            return df_union


data = ProcessDataUploadHandler()
data.setDbPathOrUrl("process.json")
data.pushDataToDb("process.json")

#query_handler = ProcessDataQueryHandler()        

#df_activities = query_handler.getAllActivities()

#pprint(df_activities)

print(data)           