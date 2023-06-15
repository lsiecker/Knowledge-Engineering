from neo4j import GraphDatabase
from decouple import config

class Neo4jClient:
    _instance = None

    @staticmethod
    def getInstance():
        if Neo4jClient._instance == None:
            Neo4jClient()
        return Neo4jClient._instance

    def __init__(self):
        if Neo4jClient._instance != None:
            raise Exception("This class is a singleton!")
        else:
            uri = '34.90.237.2'
            port = '7687'
            user = 'neo4j'
            password = '^ZC!Ft&:-:::bg5'
            self.driver = GraphDatabase.driver(f"bolt://{uri}:{port}", auth=(user, password))
            Neo4jClient._instance = self

    def close(self):
        self.driver.close()

    def execute_query(self, query, parameters={}):
        with self.driver.session() as session:
            return list(session.run(query, parameters))