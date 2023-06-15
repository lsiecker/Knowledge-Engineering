# requirements
# pip install wikidataintegrator
# pip install fuzzywuzzy
# pip install IMDbPY

import pandas as pd
from wikidataintegrator import wdi_core
from fuzzywuzzy import fuzz, process
from datetime import datetime
from tqdm import tqdm

#TODO: some actors are not found - fuzzywuzzy and P31 added - imdbpy used
#TODO: add movie_name, so we make sure we have the right actor
#TODO: what if wikidata finds actors with similar names?
#TODO: add the datetime thing from Luc

# Read the CSV file using pandas
df = pd.read_csv('data/cleaned_data/person.csv')

# Test on subset data (REMOVE THIS LINE FOR FULL DATA)
# df = df[0:50]

# Define the properties to query
properties = {
    'birth_date': 'P569',
    'death_date': 'P570',
    'occupation': 'P106',
    'start_activity': 'P2031',
    'end_activity': 'P2032',
    'instance_of': 'P31'  # Fuzzy name matching addition
}

# # Create an IMDb access object
# ia = imdb.IMDb()

# Define a function to query Wikidata for a person's information using fuzzy name matching
def query_person_info_WIKI(person_name):
    # Fuzzy name matching to find the closest match
    matching_names = process.extractOne(person_name, df['person_name'], scorer=fuzz.partial_ratio)
    matched_person_name = matching_names[0]

    query = '''
    SELECT ?person ?personLabel ?birthdate ?deathdate ?occupation ?occupationLabel ?start_activity ?end_activity
    WHERE
    {
        ?person rdfs:label "%s"@en.
        ?person wdt:%s ?birthdate.
        OPTIONAL { ?person wdt:%s ?deathdate. }
        ?person wdt:%s ?occupation.
        ?person wdt:%s ?start_activity.
        OPTIONAL { ?person wdt:%s ?end_activity. }
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    ''' % (
    matched_person_name, properties['birth_date'], properties['death_date'], properties['occupation'],
    properties['start_activity'], properties['end_activity'])

    results = wdi_core.WDItemEngine.execute_sparql_query(query)
    return results['results']['bindings']

# Create empty lists to store the queried information
person_data = []

# Iterate over the dataframe rows and query both APIs for each person
for index, row in tqdm(df.iterrows(), total=len(df), desc='Processing'):
    person_name = row['person_name']

    # Query the person information from Wikidata
    results_wiki = query_person_info_WIKI(person_name)

    # # Query the person information from IMDb
    # person_info_imdb = query_person_info_IMDb(person_name)

    # Extract the relevant information from Wikidata and store it in a dictionary
    person_info_wiki = {
        'person_name': person_name,
        'birth_date': results_wiki[0]['birthdate']['value'] if results_wiki else '',
        'death_date': results_wiki[0]['deathdate']['value'] if results_wiki and 'deathdate' in results_wiki[0] else '',
        'start_activity': results_wiki[0]['start_activity']['value'] if results_wiki else '',
        'end_activity': results_wiki[0]['end_activity']['value'] if results_wiki and 'end_activity' in results_wiki[0] else ''
    }

    # Merge the information from Wikidata and IMDb
    # if person_info_imdb:
    #     person_info_wiki.update(person_info_imdb)

    # Append the person information to the list
    person_data.append(person_info_wiki)

# Create a new dataframe from the person_data list
person_df = pd.DataFrame(person_data)

# Change dates to correct format
column_names = ['birth_date', 'death_date', 'start_activity', 'end_activity']
for column_name in column_names:
    for i in range(len(person_df)):
        person_df[column_name][i] = str(person_df[column_name][i])[:4]

# Save the dataframe to a CSV file
person_df.to_csv('data/cleaned_data/Person_extended.csv', index=False)
