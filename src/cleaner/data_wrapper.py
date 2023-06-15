from pathlib import Path
import re
import numpy as np
import pandas as pd
from tqdm import tqdm
from fuzzywuzzy import fuzz
from dateutil import parser


class DataWrapper():
    """
    A class used to represent a data wrapper.

    Attributes
    ----------
    data : pd.DataFrame
        The data to be cleaned.
    header_order : list
        The order of the headers in the data.
    name : str
        The name of the data.
        
    Methods
    -------
    get_name()
        Returns the name of the data.
    get_data()
        Returns the data.
    set_data(data) 
        Sets the data.
    get_headers()
        Returns the headers of the data.
    set_headers(*args, split_string = ['movie_name'])
        Sets the headers of the data.
    order_headers(headers)  
        Orders the headers of the data.
    format_headers(headers)
        Formats the headers of the data.
    make_date(column_name)
        Makes a date from the years and from the specific dates in the data.
    birthday(column_name)
        Makes a birthday from the years and from the specific dates in the data.
    """

    def __init__(self, data_source: Path, name: str = None) -> None:
        self.data = self.set_data(data_source)
        # The preferred order of the headers
        self.header_order = ['movie_name', 'movie_date', 'movie_rating', 'movie_genre', 'director', 'writer', 'actor', 'award_year']
        self.name = name

    def __call__(self):
        return self.data

    def get_name(self) -> str:
        """
        Function for getting the name of the data.
        
        Returns
        -------
        str
            The name of the data.
        """
        return self.name

    def get_data(self) -> pd.DataFrame:
        """
        Function for getting the data.

        Returns
        -------
        pd.DataFrame
            The data.
        """
        return self.data
    
    def set_data(self, data) -> None:
        """
        Function for setting the data.

        Parameters
        ----------
        data : Path
            The path to the data.

        Returns
        -------
        None
        """
        if not data.is_file():
            raise ValueError('This file does not exist.')
        
        # Check if is a csv file
        if str(data).endswith('.csv'):
            return pd.read_csv(data, encoding_errors='ignore')
        elif str(data).endswith('.xlsx'):
            return pd.read_excel(data)
        elif str(data).endswith('.tsv'):
            return pd.read_csv(data, sep='\t', header=0)
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

    
    def set_headers(self, *args, split_string = ['movie_name']):
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
                if header not in split_string:
                    # Split on both comma and semi-colon
                    self.data[header] = self.data[header].str.split(',|;')
                    # Make column a list
                    self.data[header] = self.data[header].apply(lambda x: x if isinstance(x, list) else [x])
          
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
            # If header ends with 's' or any other letter at the end, remove it
            if header[:-1].lower() in self.header_order:
                header = header[:-1]
            formatted_headers.append(header)
        
        return formatted_headers

    def make_date(self, column_name, target_format='%Y'):
        """
        A function that infers the date format in a column and converts it to the target format.

        Parameters
        ----------
        column_name : str
            The name of the column to be converted to a date.
        target_format : str, optional
            The desired format to convert the column to. Default is '%Y'.

        Returns
        -------
        None
        """
        # If the values are lists, extract the strings from the lists
        self.data[column_name] = self.data[column_name].apply(lambda x: x[0] if isinstance(x, list) else x)
        
        # # Replace everything that's not a digit, slash, dash, or space
        # self.data[column_name] = self.data[column_name].replace(r"[^0-9\-/ ]", "", regex=True)
        
        # Apply the date parsing and formatting
        self.data[column_name] = self.data[column_name].apply(lambda x: parser.parse(str(x)).strftime(target_format) if pd.notnull(x) else x)
        
        return
    
    def make_boolean(self, column_name, true_value, false_value):
        """
        A function that converts the values in a column to boolean values.

        Parameters
        ----------
        column_name : str
            The name of the column to be converted to boolean values.
        true_value : str
            The value to be converted to True.
        false_value : str
            The value to be converted to False.

        Returns
        -------
        None
        """
        # Convert the data in the column to boolean values
        
        self.data[column_name] = self.data[column_name].apply(lambda x: True if x == true_value else False if x == false_value else x)

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



class DataSet():
    def __init__(self, name: str) -> None:
        self.name = name
        self.data = None
        self.headers = None
        self.header_order = ['movie_name', 'movie_date', 'movie_rating', 'movie_genre', 'director', 'writer', 'actor', 'award_year', 'person_name', 'person_dateofbirth']

    def __call__(self):
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
    
    def drop_winner(self, column_name, inverse = False):
        # Function that drops the row if the value in the column_name is True
        if inverse:
            self.data = self.data[self.data[column_name] == False]
        else:
            self.data = self.data[self.data[column_name] == True]
    
    def melt_data(self, *columns):
        # Melt the columns into a single column
        # Create a new DataFrame with the 'writer', 'director', and 'actor' columns melted into separate rows
        melted_data = self.data.melt(id_vars=columns, value_vars=['person_name', 'actor', 'director', 'writer'], value_name="person_names")

        # Remove rows with missing names
        melted_data.dropna(subset=["person_names"], inplace=True)

        # Drop the 'variable' column
        melted_data.drop("variable", axis=1, inplace=True)

        # Drop the 'person_name' column and rename person_names to person_name
        melted_data.drop("person_name", axis=1, inplace=True)
        
        # Set the header names
        melted_data.columns = columns

        # reorder the columns following preferred order
        melted_data = melted_data[self.order_headers(melted_data.columns)]

        # Sort the DataFrame by 'person_name'
        melted_data.sort_values(by="person_name", inplace=True)

        # Reset the index
        melted_data.reset_index(drop=True, inplace=True)
        
        self.data = melted_data
        self.headers = self.data.columns


class DataMatcher():
    def __init__(self) -> None:
        pass

    def aggregate(self, dataframe: pd.DataFrame, *columns, automatic: bool = True, drop_nan_keys: bool = True, dif_timestamps= False):
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
        automatic : bool
            Whether to automatically aggregate the rows or not.
        drop_nan_keys : bool
            Whether to drop the rows with nan values in the columns that are matched on.
        dif_timestamps : bool
            Whether to check if the timestamps are different.

        Returns
        -------
        pd.DataFrame
            The aggregated dataframe.
        """

        # If the column consists of strings, strip the strings from the leading spaces
        for column in columns:
            if dataframe[column].dtype == 'object':
                dataframe[column] = dataframe[column].str.strip()

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

            # Check if in the duplicated rows there are nan or nat values and filter out those rows
            if drop_nan_keys:
                duplicated_rows = duplicated_rows.dropna(subset=columns, how='any')

            print(f'There are {len(duplicated_rows)} duplicate rows without nan or nat values.')

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
                    # If key or existing key are nat or nan, skip
                    if any([pd.isnull(key_value) for key_value in key]) or any([pd.isnull(existing_key_value) for existing_key_value in existing_key]):
                        continue
                    # If key or existing key contain a TimeStamp, only match if 100% similar
                    elif dif_timestamps and key[0] == existing_key[0] and any([isinstance(key_value, pd.Timestamp) for key_value in key]) and any([isinstance(existing_key_value, pd.Timestamp) for existing_key_value in existing_key]):

                        if similarity_score == 100:
                            row_combinations[existing_key].append(row_index)
                            fuzzy_match = True
                            continue
                        else:
                            fuzzy_match = False
                            print('\nDifferent timestamps: ', key, existing_key)
                            continue

                    if similarity_score == 100:
                        # If the similarity score is 100, consider them the same keys
                        row_combinations[existing_key].append(row_index)
                        fuzzy_match = True
                        continue
                    elif pattern_roman.search(key[0]) or pattern_roman.search(existing_key[0]):
                        fuzzy_match = False
                        continue
                    elif pattern_last.search(key[0]) or pattern_last.search(existing_key[0]):
                        fuzzy_match = False
                        continue
                    elif similarity_score >= 98:  # Set a threshold for similarity
                        # If the similarity score is above the threshold, consider them similar keys
                        row_combinations[existing_key].append(row_index)

                        if similarity_score < 100:
                            print(f'\nSimilar keys: {key} and {existing_key} with a similarity score of {similarity_score}.')

                        fuzzy_match = True
                        continue
                if not fuzzy_match:
                    row_combinations[key] = [row_index]

            columns = list(columns)

            # Iterate over the row combinations and handle conflicts
            for key, rows in tqdm(row_combinations.items(), desc="Handeling conflicts between rows", position=0, smoothing=0.4, leave=False):
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

