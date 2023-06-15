from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node
from src.domainmodel.neo4j_relation import Relation

@dataclass
class DirectedRelation(Relation):

    def __init__(self, person: Node, movie: Node):
        self.subject = person
        self.object = movie
        self.label = 'DIRECTED'

    def natural_keys(self) -> dict:
        return NotImplementedError("DirectedRelation.natural_keys() not implemented")