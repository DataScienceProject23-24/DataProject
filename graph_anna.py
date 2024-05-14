from rdflib import Graph, RDF, URIRef, Literal
import pandas as pd
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

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


#I create a base url to be shared among all my objects
base_url = "https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/"


#Now i read my DataFrame
CulturalHeritageObject = pd.read_csv("resources\meta.csv",
                            keep_default_na=False,
                            dtype={
                               "Id": "string",
                               "Type": "string",
                               "Title": "string",
                               "Date":"string",
                               "Author": "string",
                               "Owner": "string",
                               "Place": "string",
                            })


for idx, row in CulturalHeritageObject.iterrows():
    local_object_id = "object-" + str(idx) #i create an id for every object 

    subj = URIRef(base_url + local_object_id)

#now I populate the graph 

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
        
    
    if row["Author"]:
        authors = row["Author"].split(";") #managin multiple authors (checked)
        for author in authors:
            author_name, author_id = author.split(" (")            
            author_id = author_id[:-1] #i remove )
            subj_person = URIRef(base_url + author_id)
            

            my_graph.add((subj, hasAuthor, subj_person))
            my_graph.add((subj_person, name, Literal(author_name)))
            my_graph.add((subj_person, id, Literal(author_id)))


print(len(my_graph)) #--> 265= 237 + 14 authors + 14 author_id (correct)



store = SPARQLUpdateStore()

endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
store.open((endpoint, endpoint))

for triple in my_graph.triples((None, None, None)):
    store.add(triple)

store.close()


#cd "C:\Users\annap\Desktop\blazergraph"
#java -server -Xmx1g -jar blazegraph.jar


#PREFIX res:    <https://github.com/DataScienceProject23-24/DataProject/tree/main/resources/>
#PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#PREFIX schema: <https://schema.org/>


#SELECT*
#WHERE {
#  ?s rdf:type schema:Book.
#  ?s ?p ?o.
#}


