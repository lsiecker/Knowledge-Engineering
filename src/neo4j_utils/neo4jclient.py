from neo4j import GraphDatabase
from decouple import config
from graphdatascience import GraphDataScience

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
            uri =config('NEO4J_URI')
            port = config('NEO4J_PORT')
            user = config('NEO4J_USER')
            password = config('NEO4J_PASSWORD')
            self.driver = GraphDatabase.driver(f"bolt://{uri}:{port}", auth=(user, password))
            self.gds = GraphDataScience(f"bolt://{uri}:{port}", auth=(user, password)) 
            Neo4jClient._instance = self

    def close(self):
        self.driver.close()

    def execute_query(self, query, parameters={}):
        with self.driver.session() as session:
            return list(session.run(query, parameters))
        
    def get_gds_version(self):
        return self.gds.version()
    

neo4j_client = Neo4jClient.getInstance()
print(neo4j_client.get_gds_version())