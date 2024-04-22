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
