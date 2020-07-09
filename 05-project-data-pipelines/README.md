![ERD](png/airflow.png)

--------------------------------------------

# Data Pipelines with Apache Airflow

This project is the design and implementation of an ETL pipeline for a music streaming service to extract, process and load data using S3 buckets. The end result will be a set of dimensional tables in order to facilitate data analytics.

## Project Datasets

Data for this project consist of a dataset subset from the Million Song
Dataset containing metadata about songs and artists as well as a dataset of
simulated user activity logs for a streaming music app based on the song data.
Both datasets are in JSON format.

* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`

## Database Schema Design & ETL Pipeline

The **sparkify** database is a star schema optimized for analytic queries on user song play
behavior.  
* **Fact Table:** songplays  

* **Dimension Tables:** users, songs, artists, time


![ERD](png/03-er-diagram-star.png)


## ETL Implementation Steps

* Extract JSON data from S3
* Process data into Parquet files using Spark
* Create Fact and Dimension using joined data
* Perform test analytical queries

## Script

```bash
cd <project working directory>
python etl.py
```
