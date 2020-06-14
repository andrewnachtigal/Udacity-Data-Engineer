import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


## CREATE STAGING TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession VARCHAR,
        lastName VARCHAR,
        length VARCHAR,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INTEGER SORTKEY DISTKEY,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        user_id INTEGER
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id VARCHAR SORTKEY DISTKEY,
        artist_latitude VARCHAR,
        artist_location VARCHAR(500),
        artist_longitude VARCHAR,
        artist_name VARCHAR(500),
        duration DECIMAL(9),
        num_songs INTEGER,
        song_id VARCHAR,
        title VARCHAR(500),
        year INTEGER
        );
""")

## CREATE ANALYTICS TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY (0,1) NOT NULL PRIMARY KEY SORTKEY,
        start_time timestamp NOT NULL,
        user_id VARCHAR(45) NOT NULL DISTKEY,
        level VARCHAR(45) NOT NULL,
        song_id VARCHAR(45) NOT NULL,
        artist_id VARCHAR(45) NOT NULL,
        session_id INTEGER NOT NULL,
        location VARCHAR(100),
        user_agent VARCHAR(500)
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL PRIMARY KEY SORTKEY,
        first_name VARCHAR(45),
        last_name VARCHAR(45),
        gender VARCHAR(45),
        level VARCHAR(45)
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(45) NOT NULL PRIMARY KEY SORTKEY,
        title VARCHAR(45),
        artist_id VARCHAR(45),
        year INTEGER,
        duration DECIMAL(9)
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id INTEGER NOT NULL PRIMARY KEY SORTKEY,
        name VARCHAR(45),
        location VARCHAR(500),
        latitude DECIMAL(9),
        longitude DECIMAL(9)
        )
        diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL PRIMARY KEY SORTKEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
        );
""")

staging_events_copy = ("""
    COPY staging_events FROM '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON '{}'
    """).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    COPY staging_songs FROM '{}'
    credentials 'aws_iam_role={}'
    FORMAT AS JSON 'auto'
    STATUPDATE ON region 'us-west-2';
    """).format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN')
)

## INSERT STATEMENTS - FINAL TABLES

songplay_table_insert = ("""
    SELECT DISTINCT
            md5(se.sessionid || se.start_time) songplay_id,
            se.start_time,
            se.user_id,
            se.level,
            ss.song_id,
            s.artist_id,
            se.sessionid,
            se.location,
            se.useragent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong') AS se
        LEFT JOIN staging_songs AS ss
        ON se.song = ss.title
            AND se.artist = ss.artist_name
            AND se.length = ss.duration
""")

user_table_insert = ("""
    INSERT INTO users (
                        user_id,
                        first_name,
                        last_name,
                        gender,
                        level)

    SELECT  DISTINCT
            se.user_id AS user_id,
            se.firstName AS first_name,
            se.lastName AS last_name,
            se.gender AS gender,
            se.level AS level

    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration)

    SELECT  DISTINCT
            ss.song_id AS song_id,
            ss.title AS title,
            ss.artist_id AS artist_id,
            ss.year AS year,
            ss.duration AS duration

    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (
                        artist_id,
                        name,
                        location,
                        latitude,
                        longitude)

    SELECT  DISTINCT ss.artist_id AS artist_id,
            ss.artist_name AS name,
            ss.artist_location AS location,
            ss.artist_latitude AS latitude,
            ss.artist_longitude AS longitude

    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO time (
                        start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)

    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(week FROM start_time) AS weekday

    FROM    staging_events AS se
    WHERE se.page = 'NextSong';
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
