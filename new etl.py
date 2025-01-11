import os
import glob
import pandas as pd
from sql_queries import *
from create_tables import *
import numpy as np


conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=2544_H&m3@!")
conn.set_session(autocommit=True)
cur = conn.cursor()


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def process_song_file(cur, filepath):
    # open song file
    song_files = get_files('data/song_data')
    filepath = song_files
    df = pd.DataFrame()
    df_list = []
    for file in filepath:
        data = pd.read_json(file, lines=True)
        df_list.append(data)
    df = pd.concat(df_list, ignore_index=True)

    # insert song record
    df_songs = df.loc[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = df_songs.values
    d_list = np.array(song_data)
    song_data = d_list.tolist()
    cur.execute(song_table_insert, song_data)
    conn.commit()
    
    # insert artist record
    df_artists = df.loc[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = df_artists.values
    a_list = np.array(artist_data)
    artist_data = a_list.tolist()
    cur.execute(artist_table_insert, artist_data)
    conn.commit()


def process_log_file(cur, filepath):

    df = pd.DataFrame()

    log_files = get_files('data/log_data')
    filepath = log_files

    # open log file
    df_logs = pd.read_json(filepath, lines=True)

    df_combined = pd.concat([df, df_logs], sort=True)
    df_combined.dropna(inplace=True)

    # filter by NextSong action
    df_time = df_logs[df_logs['page'] == 'NextSong']

    # convert timestamp column to datetime
    timeframe = df_time['ts']
    t = pd.to_datetime(timeframe)
    
    # insert time data records
    time_data = (t['ts'].dt.hour, t['ts'].dt.day, t['ts'].dt.weekofyear, t['ts'].dt.month, t['ts'].dt.year, t['ts'].dt.weekday)
    column_labels = ['start_time', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(dictionary)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df_logs[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    ts = df_time['ts'].values.tolist()
    level = df_time['level'].values.tolist()
    sessionId = df_time['sessionId'].values.tolist()
    location = df_time['location'].values.tolist()
    user_agent = df_time['user_agent'].values.tolist()

    # insert songplay records
    for index, row in df_time.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(
            "SELECT songs.song_id, artists.artist_id FROM songs JOIN artists ON songs.artist_id=artists.artist_id")
        results = cur.fetchone()
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        songplay_data = (row['ts'], row['user_id'], row['level'], song_id, artist_id, row['sessionId'], row['location'],
                         row['user_agent'])

        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=2544_H&m3@!")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()