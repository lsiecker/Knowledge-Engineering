from pathlib import Path
from data_wrapper import DataWrapper, DataSet, DataMatcher

# Making DataWrappers for all the datasets
# Design choice: Drop all columns that are not used by prefixing a '_' to the column name
IMDB_top_250 = DataWrapper(Path('data\IMDB Top 250 Movies.csv'), 'IMDB Top 250 Movies')
IMDB_top_250.set_headers('_', 'movie_name', 'movie_date', 'movie_rating', 'movie_genre', 'movie_censor', 'movie_runtime', 
                         'movie_overview', 'movie_budget', 'movie_box_office', 'actors', 'directors', 'writers')
IMDB_all_genres = DataWrapper(Path('data\IMDb_All_Genres_etf_clean1.csv'), 'IMDb All Genres')
IMDB_all_genres.set_headers('movie_name', 'movie_date', 'director', 'actors', 'movie_rating', 'movie_runtime', 
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
oscar_award = DataWrapper(Path('data\\the_oscar_award.csv'), 'Oscar award')
oscar_award.set_headers('movie_date', 'award_year', '_award_ceremony_number', 'award_category', 'person_name', 'movie_name', 
                        'award_winner')
character_meta = DataWrapper(Path('data\character.metadata.tsv'), 'Character metadata')
character_meta.set_headers('_', '_', 'movie_date', 'character_name', 'person_dateofbirth', 'person_gender', '_person_height', '_person_ethnicity', 'person_name', 'person_age_movie', '_', '_', "_")
movie_meta = DataWrapper(Path('data\movie.metadata.tsv'), 'Movie metadata')
movie_meta.set_headers('_', '_', 'movie_name', 'movie_date', 'movie_revenue', 'movie_runtime', 'movie_language', 'movie_country', 'movie_genre')


# Making a list of all the datasets
# Design choice: Dropped the character metadata and movie metadata datasets, because they are big 
# and increasing the runtime of the program by hours.
datasets = [IMDB_top_250, IMDB_all_genres, movies, mymovies, oscar_demographics, oscar_award]

# Making the cleaned datasets by using a DataSet object
cleaned_movies  = DataSet('Movie')
cleaned_movies.set_headers('movie_name', 'movie_date', 'movie_censor', 'movie_genre', 'movie_rating')
cleaned_persons = DataSet('Person')
cleaned_persons.set_headers('person_name', 'person_dateofbirth', 'actor', 'director', 'writer')
cleaned_awards  = DataSet('Award')
cleaned_awards.set_headers('award_category', 'award_year')
cleaned_genres  = DataSet('Genre')
cleaned_genres.set_headers('movie_genre')

cleaned_acted_in = DataSet('Acted in')
cleaned_acted_in.set_headers('movie_name', 'movie_date', 'actor')
cleaned_directed = DataSet('Directed')
cleaned_directed.set_headers('movie_name', 'movie_date', 'director')
cleaned_wrote = DataSet('Wrote')
cleaned_wrote.set_headers('movie_name', 'movie_date', 'writer')

cleaned_nominated_for = DataSet('Nominated for (Movie)')
cleaned_nominated_for.set_headers('movie_name', 'movie_date', 'award_category', 'award_year', 'award_winner')
cleaned_won = DataSet('Won (Movie)')
cleaned_won.set_headers('movie_name', 'movie_date', 'award_category', 'award_year', 'award_winner')
cleaned_nominated_for_person = DataSet('Nominated for (Person)')
cleaned_nominated_for_person.set_headers('person_name', "award_category", 'award_year', 'award_winner')
cleaned_won_person = DataSet('Won (Person)')
cleaned_won_person.set_headers('person_name', "award_category", 'award_year', 'award_winner') 

cleaned_has_genre = DataSet('Has genre')
cleaned_has_genre.set_headers('movie_name', 'movie_date', 'movie_genre',)

# For each dataset, add data to the cleaned datasets, but only for the columns that are in the cleaned datasets
for data in datasets:
    # Making date columns datetime
    for col in ['person_dateofbirth', 'movie_date', 'movie_rating_time', 'award_year']:
        if col in data.get_headers():
            # For the given columns, make it datetime
            data.make_date(col)
    for col in ['award_winner']:
        if col in data.get_headers():
            # For the given columns, make it boolean
            data.make_boolean(col, 'True', 'False')
            data.make_boolean(col, '1', '0')
            data.make_boolean(col, 'TRUE', 'FALSE')
            data.make_boolean(col, 'golden', 'finalized')

    # For each dataset, add data to the cleaned datasets, but only for the columns that are in the cleaned datasets
    for cleaned_data in [cleaned_movies, cleaned_persons, cleaned_awards, cleaned_genres, \
                         cleaned_acted_in, cleaned_directed, cleaned_wrote, \
                         cleaned_nominated_for, cleaned_won, cleaned_nominated_for_person, \
                         cleaned_won_person, cleaned_has_genre]:

        # Get the columns that are in both datasets
        common_cols = list(set(data.get_headers()).intersection(cleaned_data.get_headers()))

        # Make a dataframe with the data from the common columns
        df = data.get_data()[common_cols]

        # Add the data to the cleaned dataset
        cleaned_data.add_data(df)

        # For the data with multiple values in one cell, explode the data (e.g. multiple actors for one movie)
        cleaned_data.explode_data()

# melt the columns of person, actor, writer and director in just one column
cleaned_persons.melt_data('person_dateofbirth', 'person_name')

# Use the DataMatcher to match the data from the different datasets
# The data is matched on the specified columns in the dataset and the cleaned datasets are updated accordingly
# The DataMatcher also drops the unknown values from the cleaned datasets for the specified columns
# The DataMatcher also exports the cleaned datasets to csv files such that they can be used in the next step of the knowledge graph.
datamatcher = DataMatcher()
# cleaned_movies.update_data(datamatcher.aggregate(cleaned_movies.get_data(), "movie_name", "movie_date", dif_timestamps=True))
# cleaned_movies.drop_unknown('movie_name', "movie_date")
# cleaned_movies.export_cleaned_data()
# cleaned_persons.update_data(datamatcher.aggregate(cleaned_persons.get_data(), "person_name"))
# cleaned_persons.drop_unknown('person_name')
# cleaned_persons.export_cleaned_data()
# cleaned_awards.update_data(datamatcher.aggregate(cleaned_awards.get_data(), "award_category", "award_year"))
# cleaned_awards.drop_unknown('award_category', "award_year")
# cleaned_awards.export_cleaned_data()
# cleaned_genres.update_data(datamatcher.aggregate(cleaned_genres.get_data(), "movie_genre"))
# cleaned_genres.drop_unknown('movie_genre')
# cleaned_genres.export_cleaned_data()

# cleaned_acted_in.update_data(datamatcher.aggregate(cleaned_acted_in.get_data(), "movie_name", "movie_date", "actor"))
# cleaned_acted_in.drop_unknown('movie_name', "movie_date", "actor")
# cleaned_acted_in.export_cleaned_data() 
# cleaned_directed.update_data(datamatcher.aggregate(cleaned_directed.get_data(), "movie_name", "movie_date", "director"))
# cleaned_directed.drop_unknown('movie_name', "movie_date", "director")
# cleaned_directed.export_cleaned_data()
# cleaned_wrote.update_data(datamatcher.aggregate(cleaned_wrote.get_data(), "movie_name", "movie_date", "writer"))
# cleaned_wrote.drop_unknown('movie_name', "movie_date", "writer")
# cleaned_wrote.export_cleaned_data()

cleaned_nominated_for.update_data(datamatcher.aggregate(cleaned_nominated_for.get_data(), "movie_name", "movie_date", "award_category", "award_year"))
cleaned_nominated_for.drop_unknown('movie_name', "movie_date", "award_category", "award_year")
cleaned_nominated_for.export_cleaned_data()
cleaned_won.update_data(datamatcher.aggregate(cleaned_won.get_data(), "movie_name", "movie_date", "award_category", "award_year"))
cleaned_won.drop_unknown('movie_name', "movie_date", "award_category", "award_year")
cleaned_won.drop_winner('award_winner')
cleaned_won.export_cleaned_data()
cleaned_nominated_for_person.update_data(datamatcher.aggregate(cleaned_nominated_for_person.get_data(), "person_name", "award_category", "award_year"))
cleaned_nominated_for_person.drop_unknown("person_name", "award_category", "award_year")
cleaned_nominated_for_person.export_cleaned_data()
cleaned_won_person.update_data(datamatcher.aggregate(cleaned_won_person.get_data(), "person_name", "award_category", "award_year"))
cleaned_won_person.drop_unknown("person_name", "award_category", "award_year")
cleaned_won_person.drop_winner('award_winner')
cleaned_won_person.export_cleaned_data()
# cleaned_has_genre.update_data(datamatcher.aggregate(cleaned_has_genre.get_data(), "movie_name", "movie_date", "movie_genre"))
# cleaned_has_genre.drop_unknown('movie_name', "movie_date", "movie_genre")
# cleaned_has_genre.export_cleaned_data()
