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

class MetadataUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()
    def pushDataToDb(self, path):
        my_graph = Graph()
        NauticalChart = URIRef("https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/NauticalChart") 
        ManuscriptPlate = URIRef("https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/ManuscriptPlate") 
        ManuscriptVolume = URIRef("https://schema.org/Manuscript")
        PrintedVolume = URIRef("https://schema.org/Book")
        PrintedMaterial = URIRef("https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/PrintedMaterial")
        Herbarium = URIRef("https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/Herbarium")
        Specimen = URIRef("https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/Specimen")
        Painting = URIRef("https://schema.org/Painting")
        Model = URIRef("https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/Model")
        Map = URIRef("https://schema.org/Map")

        #attributes
        id = URIRef("https://schema.org/identifier")
        title = URIRef("https://schema.org/title")
        date = URIRef("https://schema.org/dateCreated")
        owner = URIRef("https://schema.org/acquiredFrom")
        place = URIRef("https://schema.org/location")
        name = URIRef("https://schema.org/name")

        #relations 
        hasAuthor = URIRef("https://schema.org/author")

        #POPULATING THE RDF GRAPH 

        #I create a base url to be shared among all my objects
        base_url = "https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/"


        #Now i read my DataFrame
        CulturalHeritageObject = pd.read_csv(path, keep_default_na=False, 
                        dtype={
                            "Id" : "string",
                            "Title" : "string",
                            "Date" : "string",
                            "Author" : "string",
                            "Owner" : "string",
                            "Place" : "string",
                            "Type" : "string"
                        })

        
        #now I insert all the objects in the graph 
        for idx, row in CulturalHeritageObject.iterrows():
            local_id = "object-" + str(idx) #i create an id for every object
            subj = URIRef(base_url + local_id)
            
            #I add all the statements:
            if row["Type"] == "Nautical chart":
                my_graph.add((subj, RDF.type, NauticalChart))

            elif row["Type"] == "Manuscript plate":
                my_graph.add((subj, RDF.type, ManuscriptPlate))
            
            elif row["Type"] == "Manuscript volume":
                my_graph.add((subj, RDF.type, ManuscriptVolume))
            
            elif row["Type"] == "Printed volume":
                my_graph.add((subj, RDF.type, PrintedVolume))
            
            elif row["Type"] == "Printed material":
                my_graph.add((subj, RDF.type, PrintedMaterial))
            
            elif row["Type"] == "Herbarium":
                my_graph.add((subj, RDF.type, Herbarium))
            
            elif row["Type"] == "Specimen":
                my_graph.add((subj, RDF.type, Specimen))
            
            elif row["Type"] == "Painting":
                my_graph.add((subj, RDF.type, Painting))
            
            elif row["Type"] == "Model":
                my_graph.add((subj, RDF.type, Model))
            
            elif row["Type"] == "Map":
                my_graph.add((subj, RDF.type, Map))
            
            if row["Id"]:
                my_graph.add((subj, id, Literal(row["Id"])))
            if row["Title"]:
                my_graph.add((subj, title, Literal(row["Title"])))
            if row["Date"]:
                my_graph.add((subj, date, Literal(row["Date"])))
            if row["Owner"]:
                my_graph.add((subj, owner, Literal(row["Owner"])))
            if row["Place"]:
                my_graph.add((subj, place, Literal(row["Place"])))
            

            if row["Author"] !="":
                authors = row["Author"].split(";") #managin multiple authors (checked)
                for author in authors:
                    author_name, author_id = author.split(" (")            
                    author_id = author_id[:-1] #i remove )
                    subj_person = URIRef(base_url + author_id)
                
                    my_graph.add((subj, hasAuthor, subj_person))
                    my_graph.add((subj_person, name, Literal(author_name)))
                    my_graph.add((subj_person, id, Literal(author_id)))


        CulturalHeritageObject.pop("Author")
        #upload the graph on triplestore

        store = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl() #my proxy for putting the data in the database

        #now I open the connection 
        store.open((endpoint, endpoint))

        for triple in my_graph.triples((None, None, None)): #specyifing None I ask for all the triples 
            store.add(triple)

        store.close()

        return True

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
            norm_df[3].to_sql("Optimising", con, if_exists="replace", index=False)
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
            ?id schema:identifier '%s'.  
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


#  A N N A  #

class MetadataQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllPeople(self):                    #also from JSON DON'T KNOW HOW  ??????????
        endpoint = self.getDbPathOrUrl()
        query_getAllPeople = """
        PREFIX res: <https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?authorId ?authorName
        WHERE {
            ?s schema:identifier ?authorId.
            ?s schema:name ?authorName.
        }
        """

        df_sparql_getAllPeople = get(endpoint, query_getAllPeople, True)
        return df_sparql_getAllPeople


    def getAllCulturalHeritageObjects(self):        
        endpoint = self.getDbPathOrUrl()
        query_getAllCulturalHeritageObjects = """
        PREFIX res: <https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?object
        WHERE {
        ?object rdf:type ?type .
        }
        """
        df_sparql_getAllCulturalHeritageObjects = get(endpoint, query_getAllCulturalHeritageObjects, True)
        return df_sparql_getAllCulturalHeritageObjects


    def getAuthorsOfCulturalHeritageObject(self, objectId: str):
        endpoint = self.getDbPathOrUrl()
        query_getAuthorsOfCulturalHeritageObject = """
        PREFIX res: <https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?authorId ?authorName
        WHERE {
        ?object schema:identifier '%s'.
        ?object schema:author ?author .
        ?author schema:identifier ?authorId.
        ?author schema:name ?authorName .
        }

        """%(objectId)
        df_sparql_getAuthorsOfCulturalHeritageObject = get(endpoint, query_getAuthorsOfCulturalHeritageObject, True)
        return df_sparql_getAuthorsOfCulturalHeritageObject



    def getCulturalHeritageObjectsAuthoredBy(self, personId: str):        
        endpoint = self.getDbPathOrUrl()
        query_getCulturalHeritageObjectsAuthoredBy = """
        PREFIX res: <https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?type ?id ?title ?date ?owner ?place ?hasAuthor
        WHERE {
        ?author schema:identifier '%s'.
        ?object schema:author ?author .
        ?object rdf:type ?type.
        ?object schema:title ?title.
        ?object schema:dateCreated ?date.
        ?object schema:acquiredFrom ?owner.
        ?object schema:location ?place. 
        ?object schema:author ?hasAuthor.
        ?object schema:identifier ?id.
        }
        """%(personId) #needs to be inside " "

        df_sparql_getCulturalHeritageObjectsAuthoredBy = get(endpoint, query_getCulturalHeritageObjectsAuthoredBy, True)
        return df_sparql_getCulturalHeritageObjectsAuthoredBy
    
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


data2 = MetadataUploadHandler()
data2.setDbPathOrUrl("http://192.168.1.169:9999/blazegraph/sparql")
data2.pushDataToDb("meta.csv")

metadata_query_handler = MetadataQueryHandler()
metadata_query_handler.setDbPathOrUrl("http://192.168.1.169:9999/blazegraph/sparql")
#pprint(metadata_query_handler.getCulturalHeritageObjectsAuthoredBy("ULAN:500114874"))

#df_activities = query_handler.getAllCulturalHeritageObjects()
#pprint(df_activities)        