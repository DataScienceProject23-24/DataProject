class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id
    def getId(self):
        return self.id


class Person(IdentifiableEntity):
    def __init__(self, name):
        self.name = name
        super().__init__(self, id)
    def getName(self):
        return self.name


class CulturalHeritageObject(IdentifiableEntity):
    def __init__(self, title, date, owner, place, authors):
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.hasAuthor = set()
        for author in authors:
            self.hasAuthor.add(author)
        
        super().__init__(self, id)

    

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
        result_authors.sort()
        return result_authors
    

class NauticalChart(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)

class ManuscriptPlate(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)

class ManuscriptVolume(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)

class PrintedVolume(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)

class PrintedMaterial(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)

class Herbarium(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)

class Specimen(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)

class Painting(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)

class Model(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)

class Map(CulturalHeritageObject):
    def __init__(self,title, date, owner, place, authors):
        super().__init__(title, date, owner, place, authors)
    





    class Activity(object):
        def __init__(self, institute, person, tools, start, end, refersTo):
            #institute
            #person
            self.tool = set()
            for Tool in tools:
                self.tool.add(Tool)
            #start
            #end
            #refersTo

        #getResponsibleInstitute
        #getResponsiblePerson

        def getTools(self):
            result_tools = set()
            for Tool in self.tool:
                result_tools.add(Tool)
            return result_tools
        
        #getStartDate
        #getEndDate
        #refersTo