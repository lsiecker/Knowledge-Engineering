from src.neo4j_utils.neo4jclient import Neo4jClient
from src.domainmodel.movie_node import Movie
from src.domainmodel.person_node import Person
from src.domainmodel.oscar_node import Oscar
from src.domainmodel.actedin_relation import ActedInRelation
from src.domainmodel.movieNominated_relation import MovieNominatedForRelation
import pandas as pd

if __name__ == "__main__":

    movies_df = pd.read_csv('data/cleaned_data/merged_movies.csv')
    for index, row in movies_df.iterrows():
        # Create a movie
        title = row['movie_name']
        year = row['movie_date']
        rating = row['movie_rating']
        budget = row['budget']
        if not (budget > 0):
            print(budget)
        movie = Movie(title, year, rating, budget)
        movie.create()

    oscar_df = pd.read_csv('data/cleaned_data/Award.csv')
    for index, row in oscar_df.iterrows():
        # Create an Oscar
        category = row['award_category']
        year = row['award_year']
        oscar = Oscar(category, year)
        oscar.create()

    movieNominated_df = pd.read_csv('data/cleaned_data/Nominated for (Movie).csv')
    for index, row in movieNominated_df.iterrows():
        # Create the movie 
        movie_name = row['movie_name']
        movie_date = row['movie_date']
        movie = Movie(movie_name, movie_date, None, None)

        # Create the oscar 
        award_cat = row['award_category']
        award_year = row['award_year']
        oscar = Oscar(award_cat, award_year)

        # Create the relationship
        nominatedFor = MovieNominatedForRelation(movie, oscar)
        nominatedFor.create()
        

    
    # person_df = pd.read.csv('../../data/cleaned_data/merged_persons.csv')
    # for index, row in person_df.iterrows():
    #     print(str(index))
    #     name = row['person_name']
    #     dob =
    





