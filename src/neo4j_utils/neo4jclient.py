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
            uri =config('NEO4J_URI')
            port = config('NEO4J_PORT')
            user = config('NEO4J_USER')
            password = config('NEO4J_PASSWORD')
            self.driver = GraphDatabase.driver(f"bolt://{uri}:{port}", auth=(user, password))
            Neo4jClient._instance = self

    def close(self):
        self.driver.close()

    def execute_query(self, query, parameters={}):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return result.single()[0]

    def print_greeting(self, message):
        query = """
        CREATE (a:Greeting) 
        SET a.message = $message 
        RETURN a.message + ', from node ' + id(a)
        """
        parameters = {"message": message}
        greeting_message = self.execute_query(query, parameters)
        print(greeting_message)