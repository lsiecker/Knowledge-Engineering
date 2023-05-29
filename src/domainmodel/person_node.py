
class Person():

    def __init__(self, name):
        self.name = name

    def get_dict(self):
        dict = {
            'name': self.name
        }
        return dict
    
    def __str__(self):
        return f"<Person {self.name}>"