class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id
    def getId(self):
        return self.id


class Person(IdentifiableEntity):
    def __init__(self, name):
        self.name = name
        super().__init__(id)
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
        
        super().__init__(id)

    
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
    

class Activity(object):
    def __init__(self, institute, person, tools, start, end, refers_to):
        self.institute = institute
        self.person = person 
        self.tool = set()
        for Tool in tools:
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
    
    def __str__(self):
        return (f"Acquisition by {self.institute} (Person: {self.person}, Tools: {self.tool}, "
                f"Start: {self.start}, End: {self.end}, Refers to: {self.refers_to}, Technique: {self.technique})")

class Processing(Activity):
    pass
    
    def __str__(self):
        return (f"Processing by {self.institute} (Person: {self.person}, Tools: {self.tool}, "
                f"Start: {self.start}, End: {self.end}, Refers to: {self.refers_to})")

class Modelling(Activity):
    pass

    def __str__(self):
        return (f"Modelling by {self.institute} (Person: {self.person}, Tools: {self.tool}, "
                f"Start: {self.start}, End: {self.end}, Refers to: {self.refers_to})")


class Optimising(Activity):
    pass

    def __str__(self):
        return (f"Optimising by {self.institute} (Person: {self.person}, Tools: {self.tool}, "
                f"Start: {self.start}, End: {self.end}, Refers to: {self.refers_to})")

class Exporting(Activity):
    pass
    
    def __str__(self):
        return (f"Exporting by {self.institute} (Person: {self.person}, Tools: {self.tool}, "
                f"Start: {self.start}, End: {self.end}, Refers to: {self.refers_to})")