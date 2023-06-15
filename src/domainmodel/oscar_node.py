from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node

@dataclass
class Oscar(Node):
    category: str
    year: str

    def __init__(self, category: str, year: str):
        self.id = None
        self.label = 'Oscar'
        self.category = category
        self.year = year
        super().__init__()

    def natural_keys(self) -> dict:
        return {'category': self.category, 'year': self.year}