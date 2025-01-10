The goal of the Sparkify project is to extract the data from the app's JSON files and populating them into a PostgreSQL database so queries
 can be easily run on the data to perform analysis of who is listening to what songs in the app.
 
 My ETL pipeline consists of reading the JSON files into a Pandas dataframe using Python.  Different fields from the song and log files are extracted
 and inserted into tables in the database (users, artists, songs, time, songplays)
 
 The tables (and columns) in the database consist of: 
 - songs (song_id, title, artist_id, year, duration)
 - artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
 - users (user_id, first_name, last_name, gender, level)
 - time (start_time, hour, day, week, month, year, weekday)
 - songplays (songplay_id, start_time, level, song_id, artist_id, session_id, location, user_agent)
 
 Files in the repository consist of:
 
 etl.ipynb - Jupyter Notebook file used to create dataframes and queries to populate Python script with
 test.ipynb - Jupyter Notebook file used to test to ensure insert queries correctly inserted the required data into each table
 etl.py - Python script file that defines all the dataframes and called insert and create queries for the data to populated into the database
 sql_queries.py - Python script to assign all queries into variables to be used in etl.py
 create_tables.py - Python script used to drop and create the database and accompanying tables
 
 Python script files should be run in the following order:
 
 sql_queries.py
 create_tables.py
 etl.py
 
 They can be run from a terminal or by double clicking the icon in Windows
 
 Database Schema Design and ETL Pipeline:
 -songplays is created as a fact table and songs, artists, users, and time are all dimension tables
 -The data for songs and artists were extracted from the data/song_data folder and the songplay log data from the data/log_data folder into their own
 dataframes so it could be generated into insert and select queries to get the required information into the sparkifydb tables