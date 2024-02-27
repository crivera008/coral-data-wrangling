import pandas as pd
import os

if not os.path.exists('coral_data.pkl'):
    pre_pickle = pd.read_excel('downloaded_data/data.xlsx', sheet_name='CoralWatch Random Survey')
    pre_pickle.to_pickle('coral_data.pkl')
raw_data = pd.read_pickle('coral_data.pkl')

# Drop irrelevant columns
correct_columns = raw_data[['Activity ID', 'Latitude', 'Longitude', 'Site Name',
       'Group name', 'Participating as', 'Submitted by', 'Observation date',
       'Time', 'Light condition', 'Depth (metres)', 'Depth (feet)',
       'Water temperature (deg. C)', 'Water temperature (deg. F)', 'Activity',
       'Photo of the reef surveyed', 'Country', 'Colour Code Lightest', 'Colour Code Darkest',
       'Average.', 'Coral Type', 'Species', 'Photo']]

def aggregator(group):
    new_row = {}
    for column in group.columns:
        values = group[column]
        if len(values.unique()) == 1:
            new_row[column] = values.iloc[0]
        else:
            new_row[column] = values.tolist()
    return pd.Series(new_row)

samples = correct_columns.groupby('Activity ID').apply(aggregator).reset_index(drop=True)

# figure out which other columns to drop, which NaN values need to be filled
# aggregate by coral type