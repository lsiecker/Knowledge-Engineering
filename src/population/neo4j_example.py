from src.neo4j_utils.neo4jclient import Neo4jClient
from src.domainmodel.movie_node import Movie
from src.domainmodel.person_node import Person
from src.domainmodel.actedin_relation import ActedInRelation
import pandas as pd

if __name__ == "__main__":

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
        movie.create()

        # Push Actors
        actors = row['Actors']
        actors = actors.split(',')
        for actor in actors:
            person = Person(actor)
            person.create()
            acted_in_relation = ActedInRelation(person, movie)
            acted_in_relation.create()






