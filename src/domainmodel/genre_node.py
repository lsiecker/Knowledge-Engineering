from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node

@dataclass
class Genre(Node):
    genre: str

    def __init__(self, genre: str):
        self.id = None
        self.label = 'Genre'
        self.genre = genre
        super().__init__()

    def natural_keys(self) -> dict:
        return {'genre': self.genre}