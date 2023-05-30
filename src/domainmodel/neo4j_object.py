from abc import ABC, abstractmethod
from src.neo4j_utils.neo4jclient import Neo4jClient

"""
@Author: Sander Moonemans
"""

class Neo4jObject(ABC):

    neo4j_client = Neo4jClient.getInstance()
    label: str

    """
    Abstract base class for Neo4j Objects
    Contains the basic CRUD methods for interfacing with the graph database.
    """
    def __init__(self, id):
        self.id = id

    @abstractmethod
    def __str__(self):
        """
        Returns a string representation of the object.
        This method must be implemented by all subclasses.
        """
        pass

    @abstractmethod
    def create(self):
        """
        Creates a new instance of the object in the graph database.
        This method must be implemented by all subclasses.
        """
        pass

    @abstractmethod
    def read(self):
        """
        Retrieves an instance of the object from the graph database.
        This method must be implemented by all subclasses.
        """
        pass

    @abstractmethod
    def update(self):
        """
        Updates an instance of the object in the graph database.
        This method must be implemented by all subclasses.
        """
        pass

    @abstractmethod
    def delete(self):
        """
        Deletes an instance of the object from the graph database.
        This method must be implemented by all subclasses.
        """
        pass

    @staticmethod
    def execute_query(query, parameters={}):
        return Neo4jObject.neo4j_client.execute_query(query, parameters)
    
    @abstractmethod
    def create_index_if_not_exists(self):
        """
        Creates an index for the object in the graph database if it does not exist.
        This method must be implemented by all subclasses.
        """
        pass

    @abstractmethod
    def natural_keys(self) -> dict:
        """
        Returns a dictionary of the natural keys of the object.
        This method must be implemented by all subclasses.
        """
        pass
