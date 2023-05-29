

class Movie():

    def __init__(self, title: str, year: int, runtime: int, total_gross: str):
        self.title = title
        self.year = year
        self.runtime = runtime
        self.total_gross = total_gross

    def get_dict(self):
        dict = {
            'title': self.title,
            'year': self.year,
            'runtime': self.runtime,
            'total_gross': self.total_gross
        }
        return dict
    
    def __str__(self):
        return f"<Movie {self.title}, {self.year}>"
