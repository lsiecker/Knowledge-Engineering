from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node

@dataclass
class Person(Node):
    name: str
    dob: int
    dod: int
    start_year: int
    end_year: int

    def __init__(self, name: str, date_of_birth: int, date_of_death: int, start_year: int, end_year: int):
        self.id = None
        self.label = 'Person'
        self.name = name
        self.date_of_birth = date_of_birth
        self.date_of_death = date_of_death
        self.start_year = start_year
        self.end_year = end_year

    def natural_keys(self) -> dict:
        return {'name': self.name, 'date_of_birth': self.date_of_birth}