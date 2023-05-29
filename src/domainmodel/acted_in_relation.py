
class ActedInRelation():

    def __init__(self, person: str, movie: str ):
        self.person = person
        self.movie = movie

    def get_dict(self):
        dict = {
            'person': self.person,
            'movie': self.movie
        }
        return dict
    
    def __str__(self):
        return f"<{self.person}-[ActedInRelation]->{self.movie}>"