![](../png/airflow.png?raw=true)
![ERD](png/airflow.png)

--------------------------------------------

## Data Pipelines with Apache Airflow  

This project is the design and implementation of an ETL pipeline to enhance
data warehouse task automation and monitoring capabilities for a streaming music
service. An ETL Pipeline is a set of processes extracting data from an input
source, transforming the data, loading it into an output destination such as a
data warehouse for reporting and analysis. The Apache Airflow workflow
management platform is used to programmatically create, schedule and monitor the
workflows.  

The ETL pipeline for this project employs AWS S3 and Redshift in addition to
Apache Airflow. The resulting ETL pipeline will allow data engineering teams to
extract, process and load customer event data, and facilitate data analytics.

### Project Data  

Data for this project consist of a dataset subset from the Million Song
Dataset containing metadata about songs and artists as well as a dataset of
simulated user activity logs for a streaming music app based on the song data.
Both datasets are in JSON format.  

> **Song data:** `s3://udacity-dend/song_data`  
>  
> **Log data:** `s3://udacity-dend/log_data`

### Database Schema Design  

The database is a star schema optimized for analytic queries on user song play
behavior. The entity relationship diagram below and preliminary ETL
staging tables are shown below.

> **Fact Table:** songplays  
>  
> **Dimension Tables:** users, songs, artists, time  

![](../png/03-er-diagram-star.png?raw=true)
![ERD](png/03-er-diagram-star.png)

>  **Staging Tables**  

![](../png/03-er-diagram-staging.png?raw=true)
![ERD](png/03-er-diagram-staging.png)

### ETL Implementation Steps

> Create fact, dimension and staging tables.  
>  
> Extract JSON data from S3 and write to staging tables in Redshift.  
>  
> Copy data to star schema fact and dimension tables in using joined data.  
>  
> Perform data quality test queries.  

### Apache Airflow DAG    

In Airflow, a DAG – a Directed Acyclic Graph – is a collection of tasks to be
run to complete an data pipeline job, organized in a way that reflects their
relationships and dependencies. A DAG is defined in a Python script, which
represents the DAGs structure (tasks and their dependencies) as code. The task
dependencies for this project are configured such that the graph view
follows the flow shown in the image below.  

![](../png/airflow-etl-dag.png?raw=true)
![ERD](png/airflow-etl-dag.png)

### Project Dependencies  

> Apache Airflow  
>  
> AWS Redshift Cluster (S3 permissions)  
>  
> Create Relation Tables in Redshift prior to starting Airflow ETL  
