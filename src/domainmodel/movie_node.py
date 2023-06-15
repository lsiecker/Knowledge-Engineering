from dataclasses import dataclass, asdict
from src.domainmodel.neo4j_node import Node

@dataclass
class Movie(Node):
    title: str
    year: int
    rating: float
    budget: int

    def __init__(self, title: str, year: int, rating: float, budget: float):
        self.id = None
        self.label = 'Movie'
        self.title = title
        self.year = year
        self.rating = rating
        self.budget = budget
        super().__init__()

    def natural_keys(self) -> dict:
        return {'title': self.title, 'year': self.year}
    

            

