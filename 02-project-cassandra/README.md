![](../png/postgresql.png?raw=true)

## Database Modeling with PostgreSQL


### Project Summary
* Model and implement postgreSQL database to query business data for data analytics.
* Implement a database star schema with fact and dimension tables for an ETL pipeline to support analytical queries and business intelligence.


#### Project Data
* Million Song Dataset
    A subset of the Million Song Dataset containing metadata for a song and the song's artist in JSON format.

* Simulated User Event Data
    Simulated activity logs for a music streaming app is generated by eventsim.
    Event data for testing is in JSON format.

### Songplay Database Schema - Entity Relation Diagram
![](../png/songplays-erd.png?raw=true)

### Running Process
From the terminal, navigate to the file working directory and the following python scripts:
* create_tables.py
* etl.py


### Explanation of Project Files
* test.ipynb displays the first few rows of each table.
* create_tables.py drops and creates database tables. This file resets tables before ETL scripts are run.
* etl.ipynb is a workbook to read and processe a single file from song_data and log_data and load data into tables.
* etl.py reads and processes files from song_data and log_data and loads them into tables.
* sql_queries.py contains sql queries and is used by create_tables.py, etl.ipynb, and etl.py.