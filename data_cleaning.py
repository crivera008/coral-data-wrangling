import pandas as pd
import os 
import numpy as np
import string

#################### LOAD RAW DATA ####################
if not os.path.exists('coral_data.pkl'):
    pre_pickle = pd.read_excel('downloaded_data/data.xlsx', sheet_name='CoralWatch Random Survey')
    pre_pickle.to_pickle('coral_data.pkl')
raw_data = pd.read_pickle('coral_data.pkl')

#################### DATA CLEANING ####################

# Drop irrelevant columns
# If fahrenheit/feet desired as units, use these columns:  'Water temperature (deg. F)' and 'Depth (feet)'
desired_columns = ['Activity ID', 'Latitude', 'Longitude', 'Site Name', 'Group name', 
       'Observation date', 'Time', 'Light condition', 'Depth (metres)', 'Water temperature (deg. C)',
       'Photo of the reef surveyed', 'Colour Code Lightest', 'Colour Code Darkest', 'Average.', 
       'Coral Type', 'Species', 'Photo']
correct_columns = raw_data[desired_columns]

# Drop rows that are missing lat/long, coral data
nan_mask = correct_columns['Latitude'].isna() | correct_columns['Longitude'].isna() | correct_columns['Colour Code Darkest'].isna() | correct_columns['Colour Code Lightest'].isna() | correct_columns['Observation date'].isna()
correct_columns = correct_columns[~nan_mask]

# Replace non-essential NaN vals with description
correct_columns.fillna({'Group name':'Not recorded', 'Light condition':'Not recorded', 'Species':"Not recorded"}, inplace=True)

# Fix inaccurate/other NaN data
correct_columns.loc[correct_columns['Water temperature (deg. C)'] == 0, 'Water temperature (deg. C)'] = 'Not recorded'
correct_columns.loc[correct_columns['Group name'] == 'unknown', 'Group name'] = 'Not recorded'
# note to self: time & observation date have some NaN values (<30) -- fix with string? fix with dummy val? tbd.

# Standardizing
correct_columns['Group name'] = correct_columns['Group name'].apply(lambda x: string.capwords(x))
correct_columns.loc[correct_columns['Group name'].str.contains('Æ') | correct_columns['Group name'].str.contains('ä'), 'Group name'] = 'Not reported'
correct_columns['Observation date'] = pd.to_datetime(correct_columns['Observation date']).dt.date

def time_cleaner(time):
    if pd.isnull(time) or len(time) < 3:
        return np.NaN
    time = str(time)
    time = time.upper().replace('.', ':')
    if len(time) < 3:
        return time
    if time[1] == ':' or (len(time) == 3):
        time = '0' + time
    if time[2] != ':' and time[3].isdigit():
        time = time[0:2] + ':' + time[2:]
    hours = int(time[0:2])
    if time[-2:] == 'PM' and hours < 12:
        if len(time) < 5:
            time = str(hours + 12) + ':00'
        time = str(hours + 12) + time[2:5]
    elif time[-2:] == 'AM' and len(time) < 5:
        time = time[:-2] + ':00'
    else:
        time = time[:5]
    return time

correct_columns['Time'] = correct_columns['Time'].apply(time_cleaner)

# Combine the two photo columns
def combine_photos(row):
    pic1 = row['Photo']
    pic2 = row['Photo of the reef surveyed']
    if not pd.isnull(pic1):
        return pic1
    elif not pd.isnull(pic2):
        return pic2
    else:
        return 'Not recorded'

correct_columns['Photo'] = correct_columns.apply(combine_photos, axis=1)
correct_columns = correct_columns.drop('Photo of the reef surveyed', axis=1)

key = {"B1":"#F9FAEA","B2":"#F3F5C1","B3":"#E9EC89","B4":"#C7CF3A","B5":"#969E39","B6":"#5B7434","C1":"#F9EEEB","C2":"#F7CAC0","C3":"#F29D88","C4":"#D15B3A","C5":"#9B3120","C6":"#651A0D","D1":"#F8EEE1","D2":"#F9DCBF","D3":"#F0BD87","D4":"#D4944F","D5":"#975924","D6":"#6A3916","E1":"#F8F5E5","E2":"#F7EAC0","E3":"#F0D688","E4":"#D2AE45","E5":"#9B7D2E","E6":"#6F5521"}

def most_common_letter(row, col1, col2):
    letters = [value[0] for value in row[col1]] + [value[0] for value in row[col2]]
    return pd.Series(letters).mode().iloc[0]

def avg_coords(color_codes):
    diff_colors = set([c[0] for c in color_codes])
    avgs = []
    for c in diff_colors:
        filtered = list(filter(lambda x: x[0] == c, color_codes))
        y_vals = [int(x[1]) for x in filtered]
        mean_y = round(np.mean(y_vals))
        avg = c + str(mean_y)
        avgs.append(avg)
    return avgs
    

def agg(group):
    cols_to_agg = ['Colour Code Lightest', 'Colour Code Darkest', 'Average.', 'Species', 'Photo']
    agg_vals = {}
    for col in group.columns:
        if col not in cols_to_agg:
            agg_vals[col] = group[col].iloc[0]
        elif col == 'Photo' or col == 'Species':
            agg_vals[col] = list(filter(lambda x: x != 'Not recorded', group[col].unique()))
        elif col == 'Average.':
            agg_vals[col] = round(np.mean(group[col].tolist()), 2)
        else:
            agg_vals[col] = group[col].tolist()
    return pd.Series(agg_vals)

samples = correct_columns.groupby(['Activity ID', 'Coral Type']).apply(agg).reset_index(drop=True)
samples = samples.drop('Activity ID', axis=1)
correct_columns['Group name'].apply(lambda x: string.capwords(x))
samples['Colour Code Lightest'] = samples['Colour Code Lightest'].apply(avg_coords)
samples['Colour Code Darkest'] = samples['Colour Code Darkest'].apply(avg_coords)

def pair_letters(row):
    paired_strings = []
    sorted_light = sorted(row['Colour Code Lightest'])
    sorted_dark = sorted(row['Colour Code Darkest'])
    
    for letter1 in sorted_light:
        for letter2 in sorted_dark:
            if letter1[0] == letter2[0]:
                paired_strings.append(f"{letter1}-{letter2}")
    return paired_strings

samples['Color Range By Letter'] = samples.apply(pair_letters, axis=1)

# Add year col!
samples['Year'] = pd.to_datetime(samples['Observation date']).dt.year

samples['Calculated average color'] = samples.apply(lambda x: most_common_letter(x, 'Colour Code Lightest', 'Colour Code Darkest'), axis=1)
samples['Calculated average color'] = samples['Calculated average color'] + samples['Average.'].apply(lambda x: str(round(x)))
samples['Calculated average color'] = samples['Calculated average color'].apply(lambda x: key[x])

# Renaming columns because I like it better this way :)
clean_data = samples.rename({'Average.':'Average val', 'Colour Code Lightest': 'Lightest color code', 'Colour Code Darkest': 'Darkest color code', 'Coral Type': 'Coral type', 'Site Name': 'Site name', 'Depth (metres)': 'Depth (m)', 'Water temperature (deg. C)': 'Water temperature (C)'}, axis=1)

clean_data.to_csv('clean_data.csv', index=False)
