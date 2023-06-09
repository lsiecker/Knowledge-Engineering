from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node
from src.domainmodel.neo4j_relation import Relation

@dataclass
class HasGenreRelation(Relation):

    def __init__(self, movie: Node, genre: Node):
        self.subject = movie
        self.object = genre
        self.label = 'HAS'

    def natural_keys(self) -> dict:
        return NotImplementedError("HasGenreRelation.natural_keys() not implemented")