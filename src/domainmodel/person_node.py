from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node

@dataclass
class Person(Node):
    name: str

    def __init__(self, name: str):
        self.id = None
        self.label = 'Person'
        self.name = name

    def natural_keys(self) -> dict:
        return {'name': self.name}