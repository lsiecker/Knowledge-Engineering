
# rank,name,year,rating,genre,certificate,run_time,tagline,budget,box_office,casts,directors,writers
# Movie_Title,Year,Director,Actors,Rating,Runtime(Mins),Censor,Total_Gross,main_genre,side_genre
# name,rating,genre,year,released,score,votes,director,writer,star,country,budget,gross,company,runtime
# Release_Date,Title,Overview,Popularity,Vote_Count,Vote_Average,Original_Language,Genre,Poster_Url
# _unit_id,_golden,_unit_state,_trusted_judgments,_last_judgment_at,birthplace,birthplace:confidence,date_of_birth,date_of_birth:confidence,race_ethnicity,race_ethnicity:confidence,religion,religion:confidence,sexual_orientation,sexual_orientation:confidence,year_of_award,year_of_award:confidence,award,biourl,birthplace_gold,date_of_birth_gold,movie,person,race_ethnicity_gold,religion_gold,sexual_orientation_gold,year_of_award_gold
# year_film,year_ceremony,ceremony,category,name,film,winner

from pathlib import Path
import re
from typing import Any
from numpy import dtype
import pandas as pd
from tqdm import tqdm, trange

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
                self.data[header] = self.data[header].str.split(', ')

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
    
    def make_string(self, column_name):
        """
        A function that makes a string from a column that includes integers or floats.

        Parameters
        ----------
        column_name : str
            The name of the column to be converted to a string.

        Returns
        -------
        None
        """
        # for items in the row, if it is a number, convert it to a string
        items = [item for item in self.data[column_name]]
        self.data[column_name].update(pd.Series(items))

        # Convert all data to utf-8
        # self.data[column_name] = self.data[column_name].str.encode('utf-8').str.decode('utf-8')
        
        return
    
    def make_bool(self, column_name, true, false):
        """
        A function that makes a boolean column from a column with true and false values.

        Parameters
        ----------
        column_name : str
            The name of the column to be converted to a boolean.
        true : str
            The string that represents a true value.
        false : str
            The string that represents a false value.
        
        Returns
        -------
        None
        """
        
        # Make the column a boolean column
        self.data[column_name] = self.data[column_name].map({true: True, false: False})
        
        return
    
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

    def convert_numbers(self, column_name):
        """
        A function that converts the text numbers into numbers
        e.g. iii -> 3 or 1,000 -> 1000 or 1.000 -> 1000 or III -> 3

        Parameters
        ----------
        column_name : str
            The name of the column to be converted to numbers.

        Returns
        -------
        None
        """

        # replace all roman numerals with their number equivalent

        # list of roman numberals and their number equivalent until 10
        # TODO: Fix words starting with V
        numerals = {' X': 10, ' IX': 9, ' VIII': 8, ' VII': 7, ' VI': 6, ' V': 5, ' IV': 4, ' III': 3, ' II': 2, ' I': 1}

        # replace all roman numerals with their number equivalent
        for numeral in numerals:
            for i, items in enumerate(self.data[column_name]):
                # check if item is float or NaN
                try:
                    if items[0].endswith(numeral):
                        self.update_data(column_name, i, items[0].replace(items, str(numerals[numeral])))
                except TypeError:
                    self.data.drop(i, inplace=True)

    def update_data(self, data):
        """
        A function that updates the data in a specific row and column.
        
        Parameters
        ----------
        column_name : str
            The name of the column to be updated.
        row : int
            The row of the data to be updated.
        data : str
            The data to be updated.
        
        Returns
        -------
        None
        """
        
        self.data = data
        
        return    
        
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



import fuzzywuzzy
from fuzzywuzzy import fuzz
from itertools import combinations

class DataMatcher():
    def __init__(self, *datsets) -> None:
        self.datasets = [dataset for dataset in datsets]

    def get_datasets(self):
        return self.datasets

    def preprocess_text(self, text):
        cleaned_text = []
        for item in text:
            # Perform other preprocessing steps as needed
            cleaned_text.append(item.lower().strip())
        return cleaned_text
    
    def match(self, column_name, similarity_threshold: int = 0.93, replace_option: str = 'keep_longest_value'):
        """
        Function that matches the values of a column in one dataset to the values of a column in another dataset.
        If the values are similar enough, the value is replaced based on the specified replace option.

        Parameters
        ----------
        column_name : str
            The name of the column to be matched.
        similarity_threshold : int
            The threshold for the similarity of the two values.
        replace_option : str
            The option for replacing the values.

        Returns
        -------
        None
        """
        matched_items = []  # List to store the matched items

        for pair in tqdm(combinations(self.datasets, 2), desc='Datasets', leave=True, position=0):
            # check if the column is in both datasets
            if column_name not in pair[0].get_headers() or column_name not in pair[1].get_headers():
                continue

            data1 = pair[0].get_data()
            data2 = pair[1].get_data()
                
            for k, item1 in enumerate(data1.loc[:, column_name]):
                try:
                    cleaned_items1 = self.preprocess_text(item1)
                except TypeError:
                    # Drop the row if the item is not a string
                    # column1.drop(k, inplace=False)
                    break
                
                for l, item2 in enumerate(data2.loc[:, column_name]):
                    try:
                        cleaned_items2 = self.preprocess_text(item2)
                    except TypeError:
                        # Drop the row if the item is not a string
                        # column2.drop(l, inplace=False)
                        break

                    if len(cleaned_items1) == 1 and len(cleaned_items2) == 1:
                        ratio = fuzz.token_sort_ratio(cleaned_items1[0], cleaned_items2[0])
                        if ratio >= similarity_threshold and ratio < 100:
                            #Exclude if last part of the string is number
                            if cleaned_items1[0][-1].isdigit() or cleaned_items2[0][-1].isdigit() and ratio > 95:
                                print(f"Not matching {cleaned_items1[0]} with {cleaned_items2[0]} with a similarity of {ratio}%")
                                break
                            print(f"Matched {cleaned_items1[0]} with {cleaned_items2[0]} with a similarity of {ratio}%")
                            print(f"Replacing {item2, l} with {item1, k}")
                            # Replace the item in the shortest list with the item in the longest list
                            if replace_option == 'keep_longest_value':
                                if len(cleaned_items1[0]) > len(cleaned_items2[0]):
                                    # update the data
                                    print(f"replacing {l, data2.loc[l, column_name]} with {k, data1.loc[k, column_name]}")
                                    data2.loc[l, column_name] = cleaned_items1[0]
                                    print(f"Replaced {cleaned_items2[0]} with {cleaned_items1[0]}")
                                    matched_items.append((cleaned_items1[0], cleaned_items2[0], ratio))
                                else:
                                    # update the data
                                    print(f"replacing {data1.loc[k, column_name]} with {data2.loc[l, column_name]}")
                                    data1.loc[k, column_name] = cleaned_items2[0]
                                    matched_items.append((cleaned_items2[0], cleaned_items1[0], ratio))
                            else:
                                raise ValueError(f'The replace option {replace_option} is not valid.')
                            
            pair[0].update_data(data1)
            pair[1].update_data(data2)

        # Print the overview of matched items
        if len(matched_items) > 0:
            print("Matched Items:")
            for item in matched_items:
                    print(f"{item[0]} --> {item[1]} with a similarity of {item[2]}%")