from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node

@dataclass
class Person(Node):
    name: str
    dob: int
    dod: int
    start_year: int
    end_year: int

    def __init__(self, name: str, dob: int, dod: int, start_year: int, end_year: int):
        self.id = None
        self.label = 'Person'
        self.name = name
        self.dob = dob
        self.dod = dod
        self.start_year = start_year
        self.end_year = end_year

    def natural_keys(self) -> dict:
        return {'name': self.name, 'year of birth': self.dob, 'year of death': self.dod, 
                'activity started': self.start_year, 'activity ended': self.end_year}