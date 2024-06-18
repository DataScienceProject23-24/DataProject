# E Z G I #
class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id
    def getId(self):
        return self.id


class Person(IdentifiableEntity):
    def __init__(self, id, name):
        super().__init__(id)
        self.name = name
    def getName(self):
        return self.name

# V I R G I #
class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, id, title, date, owner, place, authors):
        super().__init__(id)
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.hasAuthor = set()
        if authors != "":
            authors_list = authors.split(";")
            auth = [i for i in authors_list]
            if len(auth)>1:
                for author in auth:
                    author_name, author_id = author.split("-")
                    self.hasAuthor.add(Person(id=author_id,name=author_name))
            else:
                author_name, author_id = auth[0].split("-")
                self.hasAuthor.add(Person(id=author_id,name=author_name))
        else:
            self.hasAuthor.add(None)

    def getTitle(self):
        return self.title
    def getDate(self):
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
    def __init__(self, institute, person, tools, start, end, refers_to):
        self.institute = institute
        self.person = person 
        self.tool = set()
        Tools = tools.split(", ")
        for Tool in Tools:
            self.tool.add(Tool)
        self.start = start
        self.end = end
        self.refers_to = refers_to
     
    def getResponsibleInsitute(self):
        return self.institute
    
    def getResponsiblePerson(self):
        return self.person

    def getTools(self):
        result_tools = set()
        for Tool in self.tool:
            result_tools.add(Tool)
        return result_tools
    
    def getStartDate(self):
        return self.start

    def getEndDate(self):
        return self.end

    def refersTo (self):
        return self.refers_to

class Acquisition(Activity):
    def __init__(self, institute, person, tools, start, end, refers_to, technique):
        self.technique = technique    
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
