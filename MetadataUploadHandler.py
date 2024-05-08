from rdflib import Graph, RDF, URIRef, Literal

my_graph = Graph()

#I create all the variables: 
#resources 
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
from pandas import read_csv, Series

#I create a base url to be shared among all my objects
base_url = "https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/"


#Now i read my DataFrame
CulturalHeritageObject = read_csv("DataProject/resources/meta.csv", keep_default_na=False, 
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


print(len(my_graph)) # --> 209 perchè senza author 



#upload the graph on triplestore
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

store = SPARQLUpdateStore()
endpoint = 'http://127.0.0.1:9999/blazegraph/sparql' #my proxy for putting the data in the database

#now I open the connection 
store.open((endpoint, endpoint))

for triple in my_graph.triples((None, None, None)): #specyifing None I ask for all the triples 
    store.add(triple)

store.close()



#cd  "C:\Users\annap\Documents\GitHub\DataProject\blazergraph"
#java -server -Xmx1g -jar blazegraph.jar
#http://127.0.0.1:9999/blazegraph/


#PREFIXES
#PREFIX res:    <https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/>
#PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#PREFIX schema: <https://schema.org/>


    
#CI SONO I DATI SU AUTHOR PERCHè è ALL'INTERNO DEL CSV ORIGINALE. 
# DEVE ESSERE MODIFICATO IN RELATION

#PROVA 
