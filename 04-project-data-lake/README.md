![](../png/aws-spark.png?raw=true)

# Data Lake with Apache Spark

Design and implement an ETL pipeline for a music streaming service to extract
data from S3 buckets, process the data using Spark, and load the data back into
S3 as a set of dimensional tables in order to facilitate analytics.

--------------------------------------------

## Project Datasets

Data for this project consist of a dataset subset from the Million Song
Dataset containing metadata about songs and artists as well as a dataset of
simulated user activity logs for a streaming music app based on the song data.
Both datasets are in JSON format.

--------------------------------------------

## Database Schema Design & ETL Pipeline

The database is a star schema optimized for analytic queries on user song play
behavior.  
**Fact Table:** songplays  
**Dimension Tables:** users, songs, artists, time

![](../png/03-er-diagram-star.png?raw=true)

--------------------------------------------

## ETL Implementation Steps

* Extract JSON data from S3
* Process data into Parquet files using Spark
* Create Fact and Dimension using joined data
* Perform test analytical queries
