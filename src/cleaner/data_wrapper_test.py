from pathlib import Path
from data_wrapper import DataWrapper, DataSet, DataMatcher

IMDB_top_250 = DataWrapper(Path('data\IMDB Top 250 Movies.csv'), 'IMDB Top 250 Movies')
IMDB_top_250.set_headers('_', 'movie_name', 'movie_date', '_movie_rating', 'movie_genre', 'movie_censor', 'movie_runtime', 
                         'movie_overview', 'movie_budget', 'movie_box_office', 'actors', 'directors', 'writers')
IMDB_all_genres = DataWrapper(Path('data\IMDb_All_Genres_etf_clean1.csv'), 'IMDb All Genres')
IMDB_all_genres.set_headers('movie_name', 'movie_date', 'director', 'actors', '_movie_rating', 'movie_runtime', 
                            'movie_censor', 'movie_gross', 'movie_genre', 'movie_side_genre')
movies = DataWrapper(Path('data\movies.csv'), 'movies')
movies.set_headers('movie_name', 'movie_censor', 'movie_genre', 'movie_date', 'movie_date', '_movie_rating', 
                   '_movie_rating_count', 'director', 'writer', 'actor', 'movie_country', 'movie_budget', 'movie_gross', 
                   'movie_company', 'movie_runtime')
mymovies = DataWrapper(Path('data\mymoviedb.csv'), 'mymoviedb')
mymovies.set_headers('movie_date', 'movie_name', '_movie_overview', 'movie_popularity', '_movie_rating_count', '_movie_rating', 
                     'movie_language', 'movie_genre', 'movie_poster')
oscar_demographics = DataWrapper(Path('data\Oscars-demographics-DFE.csv'), 'Oscars demographics')
oscar_demographics.set_headers('movie_id', 'award_winner', '_', 'movie_rating', 'movie_rating_time', 'person_birthplace', 
                               '_confidence_birthplace', 'person_dateofbirth', '_confidence_dateofbirth', 'person_race', 
                               '_confidence_race', 'person_religion', '_confidence_religion', 'person_sexualorientation', 
                               '_confidence_sexualorientation', 'award_year', '_confidence_award_year', 'award_category', 
                               'person_bio', '_birthplace_gold', '_dateofbirth_gold', 'movie_name', 'person_name', '_r', '_re', 
                               '_s', '_y' )
# Dropped race_ethnicity_gold, sexual_orientation_gold, religion_gold, year_of_award_gold, birthplace_gold, date_of_birth_gold and all confidence columns
oscar_award = DataWrapper(Path('data\\the_oscar_award.csv'), 'Oscar award')
oscar_award.set_headers('movie_date', 'award_year', '_award_ceremony_number', 'award_category', 'award_person', 'movie_name', 
                        'award_winner')


datasets = [IMDB_top_250, IMDB_all_genres, movies, mymovies, oscar_demographics, oscar_award]

cleaned_movies  = DataSet('movies')
cleaned_movies.set_headers('movie_name', 'movie_censor', 'movie_genre', 'movie_date', 'movie_rating')
cleaned_persons = DataSet('persons')
cleaned_persons.set_headers('person_name', 'person_dateofbirth', 'person_bio', 'person_race', 'person_religion', 'person_sexualorientation', 'person_birthplace')
cleaned_awards  = DataSet('awards')
cleaned_awards.set_headers('movie_name', 'award_year', 'award_category', 'award_winner', 'award_person')
cleaned_genres  = DataSet('genres')
cleaned_genres.set_headers('movie_name', 'movie_genre', 'movie_side_genre')

for data in datasets:
    # print(data.get_data().info())

    # Cleaning of the datasets
    for col in ['movie_date', 'movie_rating_time', 'person_dateofbirth', 'award_year']:
        if col in data.get_headers():
            # For the given columns, make it datetime
            data.make_date(col)

    
    

    # For each dataset, add data to the cleaned datasets, but only for the columns that are in the cleaned datasets
    # For actors, writers, directors, we need to split the data into multiple rows

    for cleaned_data in [cleaned_movies, cleaned_persons, cleaned_awards, cleaned_genres]:
        # For the columns that needs to be in the cleaned dataset
        # Copy the rows from the original dataset to the cleaned dataset for the specific columns

        # Get the columns that are in both datasets
        common_cols = list(set(data.get_headers()).intersection(cleaned_data.get_headers()))

        # Make a dataframe with the data from the common columns
        df = data.get_data()[common_cols]
        # Add the data to the cleaned dataset
        cleaned_data.add_data(df)
        cleaned_data.explode_data()

datamatcher = DataMatcher()
cleaned_movies.update_data(datamatcher.aggregate(cleaned_movies.get_data(), "movie_name", "movie_date"))
cleaned_movies.drop_unknown('movie_name', "movie_date")
cleaned_persons.update_data(datamatcher.aggregate(cleaned_persons.get_data(), "person_name"))
cleaned_persons.drop_unknown('person_name')
cleaned_awards.update_data(datamatcher.aggregate(cleaned_awards.get_data(), "movie_name", "award_year"))
cleaned_awards.drop_unknown('movie_name', "award_year")
cleaned_genres.update_data(datamatcher.aggregate(cleaned_genres.get_data(), "movie_name"))
cleaned_genres.drop_unknown('movie_name')

for cleaned_data in [cleaned_movies, cleaned_persons, cleaned_awards, cleaned_genres]:
    cleaned_data.export_cleaned_data()


