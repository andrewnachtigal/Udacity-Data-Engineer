1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
2. State and justify your database schema design and ETL pipeline.

3. [Optional] Provide example queries and results for song play analysis.


## Data Warehouse Setup for Song Play Analysis

A fictional music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Main Processing Steps

extracts data from S3
stages them in Redshift
transforms data into a set of dimensional tables (star schema) for their analytics team to continue finding insights in what songs their users are listening to
run queries for testing, given by the fictional analytics team of Sparkify

## Datasets

Song data: s3://udacity-dend/song_data Log data: s3://udacity-dend/log_data

Log data json path: s3://udacity-dend/log_json_path.json

Both the song datasets and the user log datasets are read in "json" format. The song dataset is a subset of the Million Song Dataset and the log datasets have been created by this event simulator.

## Database Design

The database is designed with a Star Schema.

The "songplays" table is the fact table with auto incrementing id and references to 4 dimension tables:

* users
* songs
* artists
* times
