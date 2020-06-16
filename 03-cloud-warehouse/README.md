![](../png/amazon-redshift.png?raw=true)

## Data Warehouse with Amazon Redshift

Design and implement a database for a music streaming service to move data and
analytic processes onto the cloud.

### Project Datasets

Data for this project consist of a subset of song data from the Million Song
Dataset and simulated app activity logs based on this song data. Song data files
are in JSON format and contains metadata about a song and the artist of that
song. Event log files are also in JSON format.

### Database Schema Design & ETL Pipeline

The database is a star schema optimized for song play analytic queries. The Fact
Table is songplays and Dimension Tables are users, songs, artists, and time.

![](../png/03-er-diagram-star.png?raw=true)
![](../png/03-er-diagram-staging.png?raw=true)

### ETL Implementation Steps

* Extract data from S3
* Copy data to staging tables in Redshift
* Transform data into a star schema with Fact and Dim tables
* Perform test analytical queries

### Running Process
From the terminal, navigate to the file working directory and run the following python scripts:
* create_tables.py
* etl.py
