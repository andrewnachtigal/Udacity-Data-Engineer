#!/usr/bin/env python3

# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays"

user_table_drop = "DROP table IF EXISTS users"

song_table_drop = "DROP table IF EXISTS songs"

artist_table_drop = "DROP table IF EXISTS artists"

time_table_drop = "DROP table IF EXISTS time"


# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays \
            (songplay_id integer PRIMARY KEY NOT NULL, \
            start_time timestamp NOT NULL, \
            user_id int NOT NULL, \
            level varchar NOT NULL, \
            song_id varchar NOT NULL, \
            artist_id varchar NOT NULL, \
            session_id int NOT NULL, \
            location varchar NOT NULL, \
            user_agent varchar NOT NULL);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users \
            (user_id int PRIMARY KEY, \
            first_name varchar NOT NULL, \
            last_name varchar NOT NULL, \
            gender varchar NOT NULL, \
            level varchar NOT NULL);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs \
            (song_id varchar PRIMARY KEY, \
            title varchar NOT NULL, \
            artist_id varchar NOT NULL, \
            year int NOT NULL, \
            duration real NOT NULL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists \
            (artist_id varchar PRIMARY KEY, \
            name varchar NOT NULL, \
            location text NOT NULL, \
            latitude real NOT NULL, \
            longitude real NOT NULL);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time \
            (start_time timestamp PRIMARY KEY, \
            hour int NOT NULL, \
            day int NOT NULL, \
            week int NOT NULL, \
            month int NOT NULL, \
            year int NOT NULL, \
            weekday int NOT NULL);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, \
                start_time, \
                user_id, \
                level, \
                song_id, \
                artist_id, \
                session_id, \
                location, \
                user_agent) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO users (user_id, \
                first_name, \
                last_name, \
                gender, \
                level) \
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT users_pkey
            DO UPDATE
            Set level = excluded.level; /* key word "excluded", refer to target column */
""")

song_table_insert = ("""
INSERT INTO songs (song_id, \
                title, \
                artist_id, \
                year, \
                duration) \
                VALUES (%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, \
                name, \
                location, \
                latitude, \
                longitude) \
                VALUES (%s, %s, %s, %s, %s);
""")


time_table_insert = ("""
INSERT INTO time (start_time, \
                hour, \
                day, \
                week, \
                month, \
                year, \
                weekday) \
                VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
/*
the log file does not specify an ID for either the song or the artist,
get the song ID and artist ID by querying the songs and artists tables to find
matches based on song title, artist name, and song duration.
*/
SELECT songs.song_id songid, artists.artist_id artistid
    FROM songs
    JOIN artists
    ON songs.artist_id = artists.artist_id
    songs.duration
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
