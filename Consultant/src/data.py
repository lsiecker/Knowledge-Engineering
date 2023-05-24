
# rank,name,year,rating,genre,certificate,run_time,tagline,budget,box_office,casts,directors,writers
# Movie_Title,Year,Director,Actors,Rating,Runtime(Mins),Censor,Total_Gross,main_genre,side_genre
# name,rating,genre,year,released,score,votes,director,writer,star,country,budget,gross,company,runtime
# Release_Date,Title,Overview,Popularity,Vote_Count,Vote_Average,Original_Language,Genre,Poster_Url
# _unit_id,_golden,_unit_state,_trusted_judgments,_last_judgment_at,birthplace,birthplace:confidence,date_of_birth,date_of_birth:confidence,race_ethnicity,race_ethnicity:confidence,religion,religion:confidence,sexual_orientation,sexual_orientation:confidence,year_of_award,year_of_award:confidence,award,biourl,birthplace_gold,date_of_birth_gold,movie,person,race_ethnicity_gold,religion_gold,sexual_orientation_gold,year_of_award_gold
# year_film,year_ceremony,ceremony,category,name,film,winner

from pathlib import Path
import pandas as pd



class DataWrapper():
    def __init__(self, data_source: Path):
        self.data = self.set_data(data_source)

    def get_data(self) -> pd.DataFrame:
        return self.data
    
    def set_data(self, data) -> None:
        if not data.is_file():
            raise ValueError('This file does not exist.')
        
        # Check if is a csv file
        if str(data).endswith('.csv'):
            return pd.read_csv(data, encoding_errors='ignore')
        elif str(data).endswith('.xlsx'):
            return pd.read_excel(data)
        else:
            raise ValueError('The data must be a csv/xlsx file.')
        
    def get_headers(self) -> list:
        """
        Function for getting the headers of the data.

        Returns
        -------
        list
            The headers of the data.
        """

        return self.data.columns.tolist()

    
    def set_headers(self, movie_title, movie_year, rating, genre, *args):
        """
        Function for setting the headers of the data.

        Parameters
        ----------
        movie_title : str
            The title of the movie.
        movie_year : str
            The year the movie was released.
        rating : str
            The rating of the movie.
        genre : str
            The genre of the movie.
        director : str
            The director of the movie.
        writer : str
            The writer of the movie.
        *args : str
            The rest of the headers.
        
        Returns
        -------
        None
        """
        # Get all the headers from the data
        headers = self.get_headers()

        # If args is empty, add current headers that are not the default ones
        if len(args) + 5 != len(headers):
            additional_headers = [header for header in headers if header not in [movie_title, movie_year, rating, genre, *args]]

        # Reorder the columns of the data, following the default headers
        self.data = self.data[[movie_title, movie_year, rating, genre, *args, *additional_headers]]

        # Rename the columns of the data, following the default headers
        self.data.columns = ['movie_title', 'movie_year', 'rating', 'genre', *args, *additional_headers]

        return
    




