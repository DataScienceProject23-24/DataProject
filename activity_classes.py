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
