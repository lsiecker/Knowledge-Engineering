from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node

@dataclass
class Person(Node):
    name: str
    dob: int

    def __init__(self, name: str, dob: int):
        self.id = None
        self.label = 'Person'
        self.name = name
        self.dob = dob

    def natural_keys(self) -> dict:
        return {'name': self.name, 'date of birth': self.dob}