from src.neo4j_utils.neo4jclient import Neo4jClient
from src.domainmodel.movie_node import Movie
from src.domainmodel.person_node import Person
from src.domainmodel.oscar_node import Oscar
from src.domainmodel.genre_node import Genre
from src.domainmodel.actedin_relation import ActedInRelation
from src.domainmodel.movieNominated_relation import MovieNominatedForRelation
from src.domainmodel.movieWon_relation import MovieHasWonRelation
from src.domainmodel.hasGenre_relation import HasGenreRelation
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

    genre_df = pd.read_csv('data/cleaned_data/Genre.csv')
    for index, row in genre_df.iterrows():
        genre = row['movie_genre']
        genre = Genre(genre)
        genre.create()

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
    
    movieWon_df = pd.read_csv('data/cleaned_data/Won (Movie).csv')
    for index, row in movieWon_df.iterrows():
        # Create the movie 
        movie_name = row['movie_name']
        movie_date = row['movie_date']
        movie = Movie(movie_name, movie_date, None, None)

        # Create the oscar 
        award_cat = row['award_category']
        award_year = row['award_year']
        oscar = Oscar(award_cat, award_year)

        # Create the relationship
        movieWon = MovieHasWonRelation(movie, oscar)
        movieWon.create()
        
    hasGenre_df = pd.read_csv('data/cleaned_data/Has genre.csv')
    for index, row in hasGenre_df.iterrows():
        # Create the movie 
        movie_name = row['movie_name']
        movie_date = row['movie_date']
        movie = Movie(movie_name, movie_date, None, None)

        # Create the genre 
        genre = row['movie_genre']
        genre = Genre(genre)

        # Create the relationship
        hasGenre = HasGenreRelation(movie, genre)
        hasGenre.create()

    





