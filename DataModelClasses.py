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
        #title
        #date
        #owner
        #place
        self.hasAuthor = set()
        for author in authors:
            self.hasAuthor.add(author)
        
        super().__init__(self, id)

    #getTitle    
    #getDate
    #getOwner
    #getPlace
    
    def getAuthors(self):
        result_authors = []
        for author in self.hasAuthor:
            result_authors.append(author)
        result_authors.sort()
        return result_authors
    

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

class Processing(Activity):
    def __init__(self, institute, person, tools, start, end, refers_to):
        super().__init__(institute, person, tools, start, end, refers_to)

class Modelling(Activity):
    def __init__(self, institute, person, tools, start, end, refers_to):
        super().__init__(institute, person, tools, start, end, refers_to)

class Optimizing(Activity):
    def __init__(self, institute, person, tools, start, end, refers_to):
        super().__init__(institute, person, tools, start, end, refers_to)

class Exporting(Activity):
    def __init__(self, institute, person, tools, start, end, refers_to):
        super().__init__(institute, person, tools, start, end, refers_to)
        