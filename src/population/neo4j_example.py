from src.neo4j_utils.neo4jclient import Neo4jClient
from src.domainmodel.movie_node import Movie
from src.domainmodel.person_node import Person
from src.domainmodel.acted_in_relation import ActedInRelation
import pandas as pd


def create_movie(movie: Movie):
    query = """
    CREATE (m:Movie {title: $title, year: $year, runtime: $runtime, total_gross: $total_gross})
    """
    parameters = movie.get_dict()
    Neo4jClient.getInstance().execute_query(query, parameters)

def create_person(person: Person):
    query = """
    CREATE (p:Person {name: $name})
    """
    parameters = person.get_dict()
    Neo4jClient.getInstance().execute_query(query, parameters)

def create_actedin_relation(actedin_relation: ActedInRelation):
    query = """
    MATCH (p:Person {name: $person})
    MATCH (m:Movie {title: $movie})
    CREATE (p)-[:ACTED_IN]->(m)
    """
    parameters = actedin_relation.get_dict()
    Neo4jClient.getInstance().execute_query(query, parameters)

def exists(class_type: str, primary_key:str, value: str):
    query = f"""
    MATCH (n:{class_type} {{{primary_key}: $value}})
    RETURN n
    """
    parameters = {
        'value': value
    }
    result = Neo4jClient.getInstance().execute_query(query, parameters)
    return len(result) > 0

if __name__ == "__main__":
    # client = Neo4jClient.getInstance()
    # client.print_greeting("Hello, World!")
    
    file_path = 'data\IMDb_All_Genres_etf_clean1.csv'
    df = pd.read_csv(file_path)
    print(df.head())
    df.to_csv('test_data\\test.csv', index=False)
    for index, row in df.iterrows():
        print(str(index))
        # Create a movie
        title = row['Movie_Title']
        year = row['Year']
        runtime = row['Runtime(Mins)']
        total_gross = row['Total_Gross']
        movie = Movie(title, year, runtime, total_gross)
        create_movie(movie)

        # Push Actors
        actors = row['Actors']
        actors = actors.split(',')
        for actor in actors:
            person = Person(actor)
            if(not exists('Person', 'name', actor)):
                create_person(person)
            acted_in_relation = ActedInRelation(actor, movie.title)
            create_actedin_relation(acted_in_relation)






