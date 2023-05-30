from dataclasses import dataclass, asdict
from src.domainmodel.neo4j_node import Node

@dataclass
class Movie(Node):
    title: str
    year: int
    runtime: int
    total_gross: str

    def __init__(self, title: str, year: int, runtime: int, total_gross: str):
        self.id = None
        self.label = 'Movie'
        self.title = title
        self.year = year
        self.runtime = runtime
        self.total_gross = total_gross

    def natural_keys(self) -> dict:
        return {'title': self.title, 'year': self.year}