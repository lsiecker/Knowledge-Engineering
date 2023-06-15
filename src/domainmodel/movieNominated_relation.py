from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node
from src.domainmodel.neo4j_relation import Relation

@dataclass
class MovieNominatedForRelation(Relation):

    def __init__(self, movie: Node, oscar: Node):
        self.subject = movie
        self.object = oscar
        self.label = 'NOMINATED_FOR'

    def natural_keys(self) -> dict:
        return NotImplementedError("HasGenreRelation.natural_keys() not implemented")