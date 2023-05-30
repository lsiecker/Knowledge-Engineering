from src.domainmodel.neo4j_object import Neo4jObject
from src.domainmodel.neo4j_node import Node
from dataclasses import dataclass, asdict

@dataclass
class Relation(Neo4jObject):
    subject: Node
    object: Node

    def __str__(self):
        return f"<{self.object}-[{self.label}]->{self.subject}>"

    def get_properties(self):
        properties = asdict(self)
        if 'subject' in properties:
            properties.pop('subject')
        if 'object' in properties:
            properties.pop('object')
        return properties
    
    def _generate_key_string_and_params(self, node: Node, alias: str):
        keys = node.natural_keys()
        keys_string = " AND ".join(f"{alias}.{key} = ${alias + '_' + key}" for key in keys.keys())
        parameters = {alias + '_' + key: value for key, value in keys.items()}
        return keys_string, parameters
    
    def create(self):
        properties = self.get_properties()
        properties_string = ", ".join(f"{key}: ${key}" for key in properties.keys())
        subject_keys_string, subject_parameters = self._generate_key_string_and_params(self.subject, 'a')
        object_keys_string, object_parameters = self._generate_key_string_and_params(self.object, 'b')
        query = f"MATCH (a:{self.subject.label}) " \
                f"WHERE {subject_keys_string} " \
                f"WITH a " \
                f"MATCH (b:{self.object.label}) " \
                f"WHERE {object_keys_string} " \
                f"MERGE (a)-[r:{self.label} {{{properties_string}}}]->(b) " \
                f"RETURN r"
        parameters = {**subject_parameters, **object_parameters, **properties}
        result = self.execute_query(query, parameters)
        return result[0] if result else None

    def read(self):
        subject_keys_string, subject_parameters = self._generate_key_string_and_params(self.subject, 'a')
        object_keys_string, object_parameters = self._generate_key_string_and_params(self.object, 'b')
        query = f"MATCH (a:{self.subject.label})-[r:{self.label}]->(b:{self.object.label}) " \
                f"WHERE {subject_keys_string} AND {object_keys_string} RETURN r"
        parameters = {**subject_parameters, **object_parameters}
        result = self.execute_query(query, parameters)
        return result[0] if result else None

    def update(self):
        properties = self.get_properties()
        properties_string = ", ".join(f"{key}: ${key}" for key in properties.keys())
        subject_keys_string, subject_parameters = self._generate_key_string_and_params(self.subject, 'a')
        object_keys_string, object_parameters = self._generate_key_string_and_params(self.object, 'b')
        query = f"MATCH (a:{self.subject.label})-[r:{self.label}]->(b:{self.object.label}) " \
                f"WHERE {subject_keys_string} AND {object_keys_string} " \
                f"SET r = {{{properties_string}}} RETURN r"
        parameters = {**subject_parameters, **object_parameters, **properties}
        result = self.execute_query(query, parameters)
        return result[0] if result else None
        
    def delete(self):
        subject_keys_string, subject_parameters = self._generate_key_string_and_params(self.subject, 'a')
        object_keys_string, object_parameters = self._generate_key_string_and_params(self.object, 'b')
        query = f"MATCH (a:{self.subject.label})-[r:{self.label}]->(b:{self.object.label}) " \
                f"WHERE {subject_keys_string} AND {object_keys_string} DELETE r"
        parameters = {**subject_parameters, **object_parameters}
        self.execute_query(query, parameters)
     
    def create_index_if_not_exists(self):
        keys_string = ", ".join(f"(r.{key})" for key in self.natural_keys())
        query = f"CREATE INDEX IF NOT EXISTS FOR ()-[r:{self.label}]-() ON {keys_string}"
        result = self.execute_query(query)
        return result[0] if result else None
