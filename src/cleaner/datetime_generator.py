import pandas as pd
#import datetime


# Read the CSV file using pandas
df = pd.read_csv('data/cleaned_data/Person_extended.csv')
# AttributeError: partially initialized module 'pandas' has no attribute 'read_csv' (most likely due to a circular import)

print(df.head())