# E Z G I #
class IdentifiableEntity(object):
    def __init__(self, id:str):
        self.id = id
    def getId(self):
        return self.id


class Person(IdentifiableEntity):
    def __init__(self, id:str, name:str):
        super().__init__(id)
        self.name = name
    def getName(self):
        return self.name

# V I R G I #
class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id:str, title:str, date:str, owner:str, place:str, authors):
        super().__init__(id)
        self.title = str(title)
        self.date = str(date)
        self.owner = str(owner)
        self.place = str(place)
        self.hasAuthor = set()
        if authors != "":
            authors_list = authors.split(";")
            auth = [i for i in authors_list]
            if len(auth)>1:
                for author in auth:
                    author_name, author_id = author.split("-")
                    self.hasAuthor.add(Person(id=str(author_id),name=str(author_name)))
            else:
                author_name, author_id = auth[0].split("-")
                self.hasAuthor.add(Person(id=str(author_id),name=str(author_name)))
        else:
            self.hasAuthor.add(None)

    def getTitle(self):
        return self.title
    def getDate(self):
        if self.date == "":
            return None
        else:
            return self.date
    def getOwner(self):
        return self.owner
    def getPlace(self):
        return self.place
    
    def getAuthors(self):
        result_authors = []
        for author in self.hasAuthor:
            result_authors.append(author)
        return result_authors

    

class NauticalChart(CulturalHeritageObject):
    pass

class ManuscriptPlate(CulturalHeritageObject):
    pass

class ManuscriptVolume(CulturalHeritageObject):
    pass

class PrintedVolume(CulturalHeritageObject):
    pass

class PrintedMaterial(CulturalHeritageObject):
    pass

class Herbarium(CulturalHeritageObject):
    pass

class Specimen(CulturalHeritageObject):
    pass

class Painting(CulturalHeritageObject):
    pass

class Model(CulturalHeritageObject):
    pass

class Map(CulturalHeritageObject):
    pass
    

# V A L E #
class Activity(object):
    def __init__(self, institute:str, person:str, tools, start:str, end:str, refers_to):
        self.institute = str(institute)
        self.person = str(person) 
        self.tool = set()
        Tools = tools.split(", ")
        for Tool in Tools:
            self.tool.add(Tool)
        self.start = str(start)
        self.end = str(end)
        self.refers_to = refers_to
     
    def getResponsibleInsitute(self):
        return self.institute
    
    def getResponsiblePerson(self):
        if self.person == "":
            return None
        else:
            return self.person

    def getTools(self):
        result_tools = set()
        for Tool in self.tool:
            result_tools.add(str(Tool))
        return result_tools
    
    def getStartDate(self):
        if self.start == "":
            return None
        else:
            return self.start

    def getEndDate(self):
        if self.end == "":
            return None
        else:
            return self.end

    def refersTo (self):
        return self.refers_to

class Acquisition(Activity):
    def __init__(self, institute:str, person:str, tools, start:str, end:str, refers_to, technique:str):
        self.technique = str(technique)    
        super().__init__(institute, person, tools, start, end, refers_to)
        
    def getTechnique(self):
        return self.technique

class Processing(Activity):
    pass

class Modelling(Activity):
    pass

class Optimising(Activity):
    pass

class Exporting(Activity):
    pass
