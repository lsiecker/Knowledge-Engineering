
# rank,name,year,rating,genre,certificate,run_time,tagline,budget,box_office,casts,directors,writers
# Movie_Title,Year,Director,Actors,Rating,Runtime(Mins),Censor,Total_Gross,main_genre,side_genre
# name,rating,genre,year,released,score,votes,director,writer,star,country,budget,gross,company,runtime
# Release_Date,Title,Overview,Popularity,Vote_Count,Vote_Average,Original_Language,Genre,Poster_Url
# _unit_id,_golden,_unit_state,_trusted_judgments,_last_judgment_at,birthplace,birthplace:confidence,date_of_birth,date_of_birth:confidence,race_ethnicity,race_ethnicity:confidence,religion,religion:confidence,sexual_orientation,sexual_orientation:confidence,year_of_award,year_of_award:confidence,award,biourl,birthplace_gold,date_of_birth_gold,movie,person,race_ethnicity_gold,religion_gold,sexual_orientation_gold,year_of_award_gold
# year_film,year_ceremony,ceremony,category,name,film,winner

from itertools import combinations
from pathlib import Path
import re
from typing import Any
import numpy as np
import pandas as pd
from tqdm import tqdm, trange
from fuzzywuzzy import fuzz


class DataSet():
    def __init__(self, name: str) -> None:
        self.name = name
        self.data = None
        self.headers = None
        self.header_order = ['movie_name', 'movie_date', 'movie_rating', 'movie_genre', 'director', 'writer', 'actor', 'award_year']

    def __call__(self) -> Any:
        return self.data
    
    def get_name(self) -> str:
        return self.name
    
    def get_data(self) -> pd.DataFrame:
        return self.data
    
    def get_headers(self) -> list:
        return self.headers
    
    def set_data(self, data: pd.DataFrame) -> None:
        self.data = data

    def set_headers(self, *headers) -> None:
        self.headers = headers
        self.order_headers(self.headers)

    def update_data(self, data: pd.DataFrame) -> None:
        self.data = data

    def add_data(self, data: pd.DataFrame) -> None:
        self.data = pd.concat([self.data, data], ignore_index=True)

    def order_headers(self, headers: list) -> list:
        """
        Function that orders the header based on a preferred predetermined order.

        Parameters
        ----------
        headers : list
            The list of headers to be ordered.

        Returns
        -------
        None
        """
        # Create a dictionary to store the order of each string in the preferred_order list
        order_dict = {name: i for i, name in enumerate(self.header_order)}
        
        # Sort the names list based on the order_dict values, using a lambda function as the key
        sorted_names = sorted(headers, key=lambda x: order_dict.get(x, float('inf')))
        
        return sorted_names
    
    def export_cleaned_data(self, format: str = 'csv', destination: Path = Path('data\cleaned_data')):
        """
        A function that exports the cleaned data to a csv file.

        Parameters
        ----------
        format : str
            The format of the file to be exported.
        path : Path
            The path of the file to be exported.

        Returns
        -------
        None
        """

        # Check if the path exists
        if not destination.is_dir():
            raise ValueError('The path does not exist.')
        
        # Check if the format is valid
        if format not in ['csv', 'xlsx']:
            raise ValueError('The format must be either csv or xlsx.')
        
        # Order the columns
        self.data = self.data[self.order_headers(self.get_headers())]

        # Export the data
        if format == 'csv':
            self.data.to_csv(destination / f'{self.name}.csv', index=False)
        elif format == 'xlsx':
            self.data.to_excel(destination / f'{self.name}.csv', index=False)
        
        return
    
    def explode_data(self):
        # For all the objects in the data, if it is a list, explode the list into multiple rows
        for header in self.data.columns:
            if self.data[header].dtype == 'object':
                # print(f'Exploding {header}')
                # Explode the list into multiple rows
                self.data = self.data.explode(header)

        return
    
    def drop_unknown(self, *columns):
        # Drop the rows with unknown values, nan values, empty strings, empty lists, empty rows if any of the columns are empty
        # Drop the empty values if any of the columns are empty
        self.data.dropna(how='any', subset=columns, inplace=True)

        return


class DataWrapper():
    def __init__(self, data_source: Path, name: str = None) -> None:
        self.data = self.set_data(data_source)
        self.header_order = ['movie_name', 'movie_date', 'movie_rating', 'movie_genre', 'director', 'writer', 'actor', 'award_year']
        self.name = name

    def __call__(self) -> Any:
        return self.data

    def get_name(self) -> str:
        return self.name

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

    
    def set_headers(self, *args):
        """
        Function for setting the headers of the data.

        Parameters
        ----------
        *args : list of the headers in the data and their new names
        
        Returns
        -------
        None
        """

        # Check if the number of headers is the same as the number of arguments
        if len(self.get_headers()) != len(args):
            raise ValueError(f'The number of headers must be the same as the number of arguments. There are {len(self.get_headers())} headers and {len(args)} arguments.')
        
        # Rename the headers of the data
        for header in args:
            self.data.rename(columns={self.get_headers()[args.index(header)]: header}, inplace=True)

        # Drop if header starts with '_'
        self.data.drop(columns=[header for header in self.get_headers() if header.startswith('_')], inplace=True)
        
        # Make the headers have first letter uppercase and the rest lowercase
        self.data.columns = self.data.columns.str.lower()
        
        # Clean the headers e.g. writers -> writer
        self.data.columns = self.format_headers(self.data.columns.tolist())

        # If column includes items in a string with a comma, split the string into a list
        for header in self.data.columns:
            if self.data[header].dtype == 'object':
                # Split on both comma and semi-colon
                self.data[header] = self.data[header].str.split(',|;')
                
          
        # Order the headers
        self.data = self.data[self.order_headers(self.get_headers())]

        return
    
    def order_headers(self, headers):
        """
        Function that orders the header based on a preferred predetermined order.

        Parameters
        ----------
        headers : list
            The list of headers to be ordered.

        Returns
        -------
        None
        """
        # Create a dictionary to store the order of each string in the preferred_order list
        order_dict = {name: i for i, name in enumerate(self.header_order)}
        
        # Sort the names list based on the order_dict values, using a lambda function as the key
        sorted_names = sorted(headers, key=lambda x: order_dict.get(x, float('inf')))
        
        return sorted_names

    def format_headers(self, headers):
        formatted_headers = []
        
        for header in headers:
            if header[:-1].lower() in self.header_order:
                header = header[:-1]
            formatted_headers.append(header)
        
        return formatted_headers

    def make_date(self, column_name):
        """
        A function that makes a date from the years and from the specific dates in the data.

        Parameters
        ----------
        column_name : str
            The name of the column to be converted to a date.
        
        Returns
        -------
        None
        """
        
        # If the column is a year, make it a date
        if self.data[column_name].dtype == 'int64':
            self.data[column_name] = pd.to_datetime(self.data[column_name], format='%Y')
        # If the column is a date, make it a date
        elif self.data[column_name].dtype == 'object':
            self.data[column_name] = pd.to_datetime(self.data[column_name], format='%Y-%m-%d', errors='coerce')
        else:
            raise ValueError(f'The column {column_name} is neither an integer nor a string.')
        
        return
    
    # def make_string(self, column_name):
    #     """
    #     A function that makes a string from a column that includes integers or floats.

    #     Parameters
    #     ----------
    #     column_name : str
    #         The name of the column to be converted to a string.

    #     Returns
    #     -------
    #     None
    #     """
    #     # for items in the row, if it is a number, convert it to a string
    #     items = [item for item in self.data[column_name]]
    #     self.data[column_name].update(pd.Series(items))

    #     # Convert all data to utf-8
    #     # self.data[column_name] = self.data[column_name].str.encode('utf-8').str.decode('utf-8')
        
    #     return
    
    # def make_bool(self, column_name, true, false):
    #     """
    #     A function that makes a boolean column from a column with true and false values.

    #     Parameters
    #     ----------
    #     column_name : str
    #         The name of the column to be converted to a boolean.
    #     true : str
    #         The string that represents a true value.
    #     false : str
    #         The string that represents a false value.
        
    #     Returns
    #     -------
    #     None
    #     """
        
    #     # Make the column a boolean column
    #     self.data[column_name] = self.data[column_name].map({true: True, false: False})
        
    #     return
    
    def drop_nan(self, column_name):
        """
        A function that drops the rows with NaN values in a column.

        Parameters
        ----------
        column_name : str
            The name of the column to be dropped.

        Returns
        -------
        None
        """
        # Drop the rows with NaN values in the column
        self.data.dropna(subset=[column_name])

        return

    # def convert_numbers(self, column_name):
    #     """
    #     A function that converts the text numbers into numbers
    #     e.g. iii -> 3 or 1,000 -> 1000 or 1.000 -> 1000 or III -> 3

    #     Parameters
    #     ----------
    #     column_name : str
    #         The name of the column to be converted to numbers.

    #     Returns
    #     -------
    #     None
    #     """

    #     # replace all roman numerals with their number equivalent

    #     # list of roman numberals and their number equivalent until 10
    #     # TODO: Fix words starting with V
    #     numerals = {' X': 10, ' IX': 9, ' VIII': 8, ' VII': 7, ' VI': 6, ' V': 5, ' IV': 4, ' III': 3, ' II': 2, ' I': 1}

    #     # replace all roman numerals with their number equivalent
    #     for numeral in numerals:
    #         for i, items in enumerate(self.data[column_name]):
    #             # check if item is float or NaN
    #             try:
    #                 if items[0].endswith(numeral):
    #                     self.update_data(column_name, i, items[0].replace(items, str(numerals[numeral])))
    #             except TypeError:
    #                 self.data.drop(i, inplace=True)

    # def update_data(self, data):
    #     """
    #     A function that updates the data in a specific row and column.
        
    #     Parameters
    #     ----------
    #     column_name : str
    #         The name of the column to be updated.
    #     row : int
    #         The row of the data to be updated.
    #     data : str
    #         The data to be updated.
        
    #     Returns
    #     -------
    #     None
    #     """
        
    #     self.data = data
        
    #     return    
        
    def export_cleaned_data(self, name: str, format: str = 'csv', destination: Path = Path('data\cleaned_data')):
        """
        A function that exports the cleaned data to a csv file.

        Parameters
        ----------
        format : str
            The format of the file to be exported.
        path : Path
            The path of the file to be exported.

        Returns
        -------
        None
        """

        # Check if the path exists
        if not destination.is_dir():
            raise ValueError('The path does not exist.')
        
        # Check if the format is valid
        if format not in ['csv', 'xlsx']:
            raise ValueError('The format must be either csv or xlsx.')
        
        # Export the data
        if format == 'csv':
            self.data.to_csv(destination / f'{name}.csv', index=False)
        elif format == 'xlsx':
            self.data.to_excel(destination / f'{name}.xlsx', index=False)
        
        return



# import fuzzywuzzy
# from fuzzywuzzy import fuzz
# from itertools import combinations


class DataMatcher():
    def __init__(self) -> None:
        pass

    def aggregate(self, dataframe: pd.DataFrame, *columns, automatic: bool = True):
        """
        A function that looks for duplicate rows in a dataframe and aggregates them.
        It is possible to have different columns filled in for the same given header.
        It will be matched on the columns that are given.
        If there are conflicts, it will print these conflicts and ask for user input.

        Parameters
        ----------
        dataframe : pd.DataFrame
            The dataframe to be aggregated.
        *columns : list
            The columns to be matched on.

        Returns
        -------
        pd.DataFrame
            The aggregated dataframe.
        """

        # Check if there are duplicate rows
        if dataframe.duplicated(subset=columns).any():
            # Get the duplicated rows
            duplicated_rows = dataframe[dataframe.duplicated(subset=columns)]
            # Get the unique rows
            unique_rows = dataframe[~dataframe.duplicated(subset=columns)]

            other_columns = [column for column in dataframe.columns if column not in columns]

            print(f'There are {len(duplicated_rows)} duplicate rows.'
                f'\nThese rows will be aggregated.')
            print(f'There are {len(unique_rows)} unique rows.'
                f'\nThese rows will be kept.')

            # Compare the duplicate rows
            # Create a dictionary to store row combinations based on the column values
            row_combinations = {}
            pattern_roman = re.compile(r"\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\b")  # Regex pattern for matching Roman numerals
            pattern_last = re.compile(r"(\D+)\d+$")  # Regex pattern for matching the non-numeric part of the string at the end

            for row_index, row_values in tqdm(duplicated_rows.iterrows(), total=duplicated_rows.shape[0], desc="Decreasing the row combinations"):
                key = tuple(row_values[list(columns)].values)
                fuzzy_match = False
                for existing_key in row_combinations.keys():
                    # Calculate the similarity score between the keys using fuzzy matching
                    similarity_score = fuzz.ratio(key, existing_key)
                    try:
                        if similarity_score == 100:
                            # If the similarity score is 100, consider them the same keys
                            row_combinations[existing_key].append(row_index)
                            fuzzy_match = True
                            break
                        elif pattern_roman.search(key[0]) or pattern_roman.search(existing_key[0]):
                            fuzzy_match = False
                        elif pattern_last.search(key[0]) or pattern_last.search(existing_key[0]):
                            fuzzy_match = False
                        elif similarity_score >= 98:  # Set a threshold for similarity
                            # If the similarity score is above the threshold, consider them similar keys
                            row_combinations[existing_key].append(row_index)

                            if similarity_score < 100:
                                print(f'\nSimilar keys: {key} and {existing_key} with a similarity score of {similarity_score}.')

                            fuzzy_match = True
                            break
                    except:
                        print(f'Error: {key} and {existing_key}')
                if not fuzzy_match:
                    row_combinations[key] = [row_index]

            columns = list(columns)

            # Iterate over the row combinations and handle conflicts
            for key, rows in tqdm(row_combinations.items(), desc="Handeling conflicts between rows"):
                try:
                    row1 = rows[0]
                    row2 = rows[1]

                    # Get the values of the other columns
                    row1_values = dataframe.loc[row1, ~dataframe.columns.isin(columns)].values
                    row2_values = dataframe.loc[row2, ~dataframe.columns.isin(columns)].values

                    # If the values are not the same, print the values and ask for user input
                    if not np.array_equal(row1_values, row2_values):
                        # Ask for user input
                        if automatic:
                            for i, col in enumerate(other_columns):
                                if dataframe.loc[row1, col] is str:
                                    dataframe.loc[row1, col] = row1_values[i] + row2_values[i]
                                elif dataframe.loc[row1, col] is list:
                                    dataframe.loc[row1, col] = row1_values[i] + row2_values[i]
                                elif dataframe.loc[row1, col] is float:
                                    dataframe.loc[row1, col] = max(row1_values[i], row2_values[i])
                                else:
                                    dataframe.loc[row1, col] = row1_values[i]
                            dataframe.drop(row2, inplace=False)
                        else:
                            tqdm.write(f'\nThere is a conflict for the following information:')
                            tqdm.write(f'\n{columns}\n{dataframe.loc[row1, columns].values}')
                            tqdm.write(f'\nWhich row do you want to keep?\n1: {row1_values}\n2: {row2_values}')

                            user_input = input('Which value do you want to keep? (1/2/3(Both)): ')
                            # If the user input is 1, keep the first row, else keep the second row
                            if user_input == '1':
                                dataframe.loc[row2, ~dataframe.columns.isin(columns)] = row1_values
                                dataframe.drop(row1, inplace=False)
                            elif user_input == '2':
                                dataframe.loc[row1, ~dataframe.columns.isin(columns)] = row2_values
                                dataframe.drop(row2, inplace=False)
                            elif user_input == '3':
                                # If the user input is 3, keep both values but in one row
                                for i, col in enumerate(other_columns):
                                    if dataframe.loc[row1, col] is str:
                                        dataframe.loc[row1, col] = row1_values[i] + row2_values[i]
                                    if dataframe.loc[row1, col] is list:
                                        dataframe.loc[row1, col] = row1_values[i] + row2_values[i]
                                    if dataframe.loc[row1, col] is float:
                                        dataframe.loc[row1, col] = max(row1_values[i], row2_values[i])
                                    if dataframe.loc[row1, col] is object:
                                        dataframe.loc[row1, col] = row1_values[i]
                                dataframe.drop(row2, inplace=False)
                            tqdm.write(f"New data: {dataframe.loc[row1].values}")
                except:
                    continue

            # Concatenate the unique rows and the updated dataframe
            dataframe = pd.concat([unique_rows, dataframe], ignore_index=True)

            # Drop the duplicate rows
            dataframe.drop_duplicates(subset=columns, inplace=True)
        return dataframe

