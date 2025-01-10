import os
import glob
import pandas as pd
from sql_queries import *
from create_tables import *
import numpy as np

# Run create_tables.py prior to running etl.py
conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=2544_H&m3@!")
conn.set_session(autocommit=True)
cur = conn.cursor()


# Function to find JSON files in data directories

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


# Extract data out of each JSON, append to list, then concat list to dataframe
filepath = get_files('data/song_data')
df = pd.DataFrame()
df_list = []
for file in filepath:
    data = pd.read_json(file, lines=True)
    df_list.append(data)
df = pd.concat(df_list, ignore_index=True)

# Create subset of dataframe for inserting data into songs table
df_songs = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
song_data = df_songs.values
d_list = np.array(song_data)
song_data = d_list.tolist()
for song in song_data:
    cur.execute(song_table_insert, song)
    conn.commit()

# Create subset of dataframe for inserting data into artists table
df_artists = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
artist_data = df_artists.values
a_list = np.array(artist_data)
artist_data = a_list.tolist()

for artist in artist_data:
    cur.execute(artist_table_insert, artist)
    conn.commit()
conn.commit()

# Create list to extract the log JSON data into
df_log_list = []
log_files = get_files('data/log_data')
df_logs = pd.DataFrame()

# Append list data into dataframe for logs
for log in log_files:
    logs = pd.read_json(log, lines=True)
    df_log_list.append(logs)
df_logs = df_logs._append(df_log_list, ignore_index=True)
df_logs.rename(columns={'artist': 'artist_name', 'firstName': 'first_name', 'itemInSession':
                        'item_in_session', 'lastName': 'last_name', 'sessionID': 'session_id',
                        'userAgent': 'user_agent', 'userId': 'user_id'}, inplace=True)

# Convert timestamp column to datetime
np_time_array = np.array(df_logs['ts'])
df_logs['ts'] = pd.to_datetime(np_time_array, unit='ms')
df_time = df_logs[df_logs['page'] == 'NextSong']

time_copy = df_time['ts']
time_copy2 = pd.DataFrame(time_copy)

# Extract individual date elements to be inserted into time table
time_data = (time_copy2['ts'], df_time['ts'].dt.hour, df_time['ts'].dt.day, df_time['ts'].dt.day_of_year,
             df_time['ts'].dt.month, df_time['ts'].dt.year, df_time['ts'].dt.weekday)
column_labels = ['start_time', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
dictionary = dict(zip(column_labels, time_data))
time_df = pd.DataFrame(dictionary)

for i, row in time_df.iterrows():
    cur.execute(time_table_insert, row)
    conn.commit()

# Insert records into users table
user_df = df_logs[['user_id', 'first_name', 'last_name', 'gender', 'level']]
user_df = user_df.dropna()

for i, row in user_df.iterrows():
    cur.execute(user_table_insert, row)
    conn.commit()

for index, row in df_time.iterrows():
    # get songid and artistid from song and artist tables

    cur.execute("SELECT songs.song_id, artists.artist_id FROM songs JOIN artists ON songs.artist_id=artists.artist_id")
    results = cur.fetchone()

    if results:
        song_id, artist_id = results
    else:
        song_id, artist_id = None, None

    songplay_data = (row['ts'], row['user_id'], row['level'], song_id, artist_id, row['sessionId'], row['location'],
                     row['user_agent'])
    # Insert records into songplay table
    cur.execute(songplay_table_insert, songplay_data)
    conn.commit()
