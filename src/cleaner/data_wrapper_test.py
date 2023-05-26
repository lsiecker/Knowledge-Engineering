from pathlib import Path
from data_wrapper import DataWrapper

IMDB_top_250 = DataWrapper(Path('Consultant\Datasets\IMDB Top 250 Movies.csv'))
IMDB_top_250.set_headers('name', 'year', 'rating', 'genre', 'directors', 'writers')
print(IMDB_top_250.get_headers())

IMDB_all_genres = DataWrapper(Path('Consultant\Datasets\IMDb_All_Genres_etf_clean1.csv'))
IMDB_all_genres.set_headers('Movie_Title', 'Year', 'Rating', 'main_genre', 'Director')
print(IMDB_all_genres.get_headers())

movies = DataWrapper(Path('Consultant\Datasets\movies.csv'))
movies.set_headers('name', 'year', 'rating', 'genre', 'director', 'writer')
print(movies.get_headers())

mymovies = DataWrapper(Path('Consultant\Datasets\mymoviedb.csv'))
mymovies.set_headers('Title', 'Release_Date', 'Vote_Average', 'Genre')
print(mymovies.get_headers())

oscar_demographics = DataWrapper(Path('Consultant\Datasets\Oscars-demographics-DFE.csv'))
print(oscar_demographics.get_headers())

oscar_award = DataWrapper(Path('Consultant\Datasets\\the_oscar_award.csv'))
print(oscar_award.get_headers())


