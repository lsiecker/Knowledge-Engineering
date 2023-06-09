# requirements
# pip install wikidataintegrator
# pip install fuzzywuzzy
# pip install IMDbPY

import pandas as pd
from wikidataintegrator import wdi_core
from fuzzywuzzy import fuzz, process
from tqdm import tqdm

#TODO: some actors are not found - fuzzywuzzy and P31 added - imdbpy used
#TODO: add movie_name, so we make sure we have the right actor
#TODO: what if wikidata finds actors with similar names?
#TODO: add the datetime thing from Luc

# Read the CSV file using pandas
df = pd.read_csv('data/cleaned_data/Movie.csv')

# Test on subset data (REMOVE THIS LINE FOR FULL DATA)
#df = df[0:10]

# Define the properties to query MORE PROPS
properties = {
    'budget': 'P2130',
    'revenue': 'P2139',
}

# Define a function to query Wikidata for a movie's information using fuzzy name matching
def query_movie_info(movie_name):
    # Fuzzy name matching to find the closest match
    matching_names = process.extractOne(movie_name, df['movie_name'], scorer=fuzz.partial_ratio)
    matched_movie_name = matching_names[0]

    query = '''
    SELECT ?movie ?movieLabel ?budget ?revenue
    WHERE
    {
        ?movie rdfs:label "%s"@en.
        OPTIONAL { ?movie wdt:%s ?budget. }
        OPTIONAL { ?movie wdt:%s ?revenue. }
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    ''' % (matched_movie_name, properties['budget'], properties['revenue'])

    results = wdi_core.WDItemEngine.execute_sparql_query(query)
    return results['results']['bindings']

# Create empty lists to store the queried information
movie_data = []
no_nobudget = 0
names_nobudget = []

# Iterate over the elements of the Series and query Wikidata for each movie
for movie_name in tqdm(df['movie_name']):
    # Query the movie information
    results = query_movie_info(movie_name)

    # Extract the relevant information and store it in a dictionary
    movie_info = {
        'movie_name': movie_name,
        'budget': results[0]['budget']['value'] if results and 'budget' in results[0] else '',
        'revenue': results[0]['revenue']['value'] if results and 'revenue' in results[0] else ''
    }
      
    if not movie_info['budget']:
        no_nobudget +=1
        names_nobudget.append(movie_name)

    # Append the movie information to the list
    movie_data.append(movie_info)

# Create a new DataFrame from the movie_data list
movie_df = pd.DataFrame(movie_data)

# Save the DataFrame to a CSV file
movie_df.to_csv('Movie_extended.csv', index=False)
movie_df.to_csv('data/cleaned_data/Movie_extended.csv', index=False)

print('We did not find budget for ', no_nobudget, ' movies. The movie names we did not find budget for are: ')
for movie in names_nobudget:
    print(movie)


# EXAMPLE: Schindler's List -> [("Schindler's List", 100, 5), ('Shine', 80, 1820), ('Lisa', 75, 6823), ('Wind', 75, 7262), ('Eros', 75, 8499)]
    # # DOES NOT WORK
    # # # If budget is not found, perform another search for matched movie names
    # if not movie_info['budget']:
    #     matched_movie_names = []
    #     # Search for matched movie names in Wikidata using fuzzy name matching
    #     matched_names = process.extract(movie_name, df['movie_name'], scorer=fuzz.partial_ratio)
    #     print(movie_name)
    #     print(matched_names)
    #     for matched_movie_name in matched_names:
    #         if matched_movie_name != movie_name:
    #             matched_results = query_movie_info(matched_movie_name)
    #             if matched_results and 'budget' in matched_results[0]:
    #                 movie_info['budget'] = matched_results[0]['budget']['value']
    #                 break
    #             else:
    #                 matched_movie_names.append(matched_movie_name)
    #     print(f"Budget for {movie_name} was not found.")
    #     print("Matched movie names are:", matched_movie_names)
    #     if matched_movie_names:
    #         print("Budget was not found for these matched movie names.")



 