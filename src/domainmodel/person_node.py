from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node
import math

def convert_to_int_or_none(x):
    try:
        return int(x)
    except (ValueError, TypeError):
        return None

@dataclass
class Person(Node):
    name: str
    birth_year: int
    death_year: int
    start_year: int
    end_year: int

    def __init__(self, name: str, birth_year: int, death_year: int, start_year: int, end_year: int):
        self.id = None
        self.label = 'Person'
        self.name = name
        self.birth_year = convert_to_int_or_none(birth_year)
        self.death_year = convert_to_int_or_none(death_year)
        self.start_year = convert_to_int_or_none(start_year)
        self.end_year = convert_to_int_or_none(end_year)
        super().__init__()


    def natural_keys(self) -> dict:
        return {'name': self.name}