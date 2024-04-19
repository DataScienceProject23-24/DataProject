class CulturalHeritageObject(object):
    def __init__(self, internal_id, title, external_id):
        self.internal_id = internal_id
        self.title = title
        self.external_id = external_id


class Activity(object):
    def __init__(self, internal_id, institute, person, tools, start, end):
        self.internal_id = internal_id
        self.institute = institute
        self.person = person 
        self.tool = set()
        for Tool in tools:
            self.tool.add(Tool)
        self.start = start
        self.end = end

         

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
        for obj in CulturalHeritageObject:
            if obj.external_id == self.internal_id:
                return obj

writing = Activity(internal_id= "2", institute="Unibo", person="Jimmy", tools={"pen", "pencil"}, start="Yesterday", end="Tomorrow")
book = CulturalHeritageObject(internal_id= "1",title="ciao", external_id="2")

print(writing.refersTo())

class Acquisition(Activity):
    def __init__(self, institute, person, tools, start, end, refersTo, technique):
        self.technique = technique    
        super().__init__(institute, person, tools, start, end, refersTo)
        
    def getTechnique(self):
        return self.technique

class Processing(Activity):
    def __init__(self, institute, person, tools, start, end, refersTo):
        super().__init__(institute, person, tools, start, end, refersTo)

class Modelling(Activity):
    def __init__(self, institute, person, tools, start, end, refersTo):
        super().__init__(institute, person, tools, start, end, refersTo)

class Optimizing(Activity):
    def __init__(self, institute, person, tools, start, end, refersTo):
        super().__init__(institute, person, tools, start, end, refersTo)

class Exporting(Activity):
    def __init__(self, institute, person, tools, start, end, refersTo):
        super().__init__(institute, person, tools, start, end, refersTo)

