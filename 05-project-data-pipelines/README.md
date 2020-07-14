![](../png/airflow.png?raw=true)
![ERD](png/airflow.png)

--------------------------------------------

## Data Pipelines with Apache Airflow  

The goal of this project is to design and implement an ETL solution to enable
data warehouse task automation and monitoring capabilities. Apache
Airflow is a workflow management that allows data engineering teams to
programmatically create, schedule and monitor complex workflows. This data
engineering solution for a streaming music service company will employ
AWS S3 and Redshift in addition to Apache Airflow. The resulting ETL pipeline
will allow a streaming service to extract, process and load customer
event data, and facilitate data analytics.

## Project Data  

Data for this project consist of a dataset subset from the Million Song
Dataset containing metadata about songs and artists as well as a dataset of
simulated user activity logs for a streaming music app based on the song data.
Both datasets are in JSON format.  

> Song data: `s3://udacity-dend/song_data`
> Log data: `s3://udacity-dend/log_data`

## Database Schema Design  

The **sparkify** database is a star schema optimized for analytic queries on
user song play behavior.  

> **Fact Table:** songplays  
> **Dimension Tables:** users, songs, artists, time

![](../png/03-er-diagram-star.png?raw=true)
![ERD](png/03-er-diagram-star.png)

## Apache Airflow Directed Acyclic Graph ETL  

![](../png/airflow-etl-dag.png?raw=true)
![ERD](png/airflow-etl-dag.png)


## Dependencies  

> Apache Airflow Installation  
> AWS Redshift Cluster (S3 permissions)  
> Create Relation Tables in Redshift prior to starting Airflow ETL

## ETL Process  

> Extract JSON data from S3 and write to staging tables
> Create Fact and Dimension using joined data
> Run test analyts queries
