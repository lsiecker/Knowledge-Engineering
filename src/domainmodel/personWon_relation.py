from dataclasses import dataclass
from src.domainmodel.neo4j_node import Node
from src.domainmodel.neo4j_relation import Relation

@dataclass
class PersonHasWonRelation(Relation):

    def __init__(self, person: Node, oscar: Node):
        self.subject = person
        self.object = oscar
        self.label = 'HAS_WON'

    def natural_keys(self) -> dict:
        return NotImplementedError("PersonHasWonRelation.natural_keys() not implemented")