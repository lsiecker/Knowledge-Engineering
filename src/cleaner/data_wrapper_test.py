from pathlib import Path
from data_wrapper import DataWrapper, DataMatcher

IMDB_top_250 = DataWrapper(Path('data\IMDB Top 250 Movies.csv'), 'IMDB Top 250 Movies')
IMDB_top_250.set_headers('_', 'movie_name', 'movie_date', 'movie_rating', 'movie_genre', 'movie_censor', 'movie_runtime', 
                         'movie_overview', 'movie_budget', 'movie_box_office', 'actors', 'directors', 'writers')
IMDB_all_genres = DataWrapper(Path('data\IMDb_All_Genres_etf_clean1.csv'), 'IMDb All Genres')
IMDB_all_genres.set_headers('movie_name', 'movie_date', 'director', 'actors', 'movie_rating', 'movie_runtime', 
                            'movie_censor', 'movie_gross', 'movie_genre', 'movie_side_genre')
movies = DataWrapper(Path('data\movies.csv'), 'movies')
movies.set_headers('movie_name', 'movie_censor', 'movie_genre', 'movie_year', 'movie_date', 'movie_rating', 
                   'movie_rating_count', 'director', 'writer', 'actor', 'movie_country', 'movie_budget', 'movie_gross', 
                   'movie_company', 'movie_runtime')
mymovies = DataWrapper(Path('data\mymoviedb.csv'), 'mymoviedb')
mymovies.set_headers('movie_date', 'movie_name', 'movie_overview', 'movie_popularity', 'movie_rating_count', 'movie_rating', 
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
oscar_award.set_headers('movie_date', 'award_year', 'award_ceremony_number', 'award_category', 'person_name', 'movie_name', 
                        'award_winner')

datasets = [IMDB_top_250, IMDB_all_genres] #, movies, mymovies, oscar_demographics, oscar_award]

for data in datasets:
    # Make all dates a date time
    for col in ['movie_date', 'movie_rating_time', 'person_dateofbirth', 'award_year', 'movie_year']:
        if col in data.get_headers():
            data.make_date(col)
    for col in ['movie_name']:
        if col in data.get_headers():
            data.drop_nan(col)
    # for col in ['movie_name', 'director', 'actors', 'writers', 'writer', 'actor', 'movie_company', 'movie_genre', 'movie_side_genre']:
    #     if col in data.get_headers():
    #         data.make_string(col)
    for col in ['movie_name']:
        if col in data.get_headers():
            data.convert_numbers(col)
    
    # print(data.get_headers())
    # print(data.get_data().head())
    # print(data.get_data().info())


dataMatcher = DataMatcher(*datasets)
# make it check for the header to be in the dataset

dataMatcher.match('movie_name', 93, 'keep_longest_value')
dataMatcher.match('person_name', 91, 'keep_longest_value')
dataMatcher.match('director', 91, 'keep_longest_value')
dataMatcher.match('actors', 91, 'keep_longest_value')
dataMatcher.match('writers', 91, 'keep_longest_value')

for data in datasets:
    # Export the cleaned_data
    data.export_cleaned_data(data.get_name() + '_cleaned.csv')