
class Person(object):
    def __init__(self, name):
        self.name = name

    def getName(self):
        return self.name


class CulturalHeritageObject(object):
    def __init__(self, title, date, owner, place, authors):
        self.title = title
        self.date = date
        self.owner = owner
        self.place = place
        self.hasAuthor = set()
        for author in authors:
            self.hasAuthor.add(author)
        

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
    
author_1 = Person(name="Jon")
object_1 = CulturalHeritageObject(title="mio", date=1482, owner="me", place="here", authors=["pip"])
print(object_1.getAuthors())    

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