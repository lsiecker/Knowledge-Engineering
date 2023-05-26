from src.neo4j_utils.neo4jclient import Neo4jClient

if __name__ == "__main__":
    client = Neo4jClient.getInstance()
    client.print_greeting("Hello, World!")