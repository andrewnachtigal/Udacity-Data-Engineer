import configparser
import os

from datetime    import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format

from pyspark.sql.functions import to_timestamp
from pyspark.sql.types     import IntegerType
from pyspark.sql           import Row, functions as F
from pyspark.sql.window    import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    Create Apache Spark session.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Function reads song and artist data from JSON source file and writes
    data to parquet file.
    '''
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df_song = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song.select(['song_id', 'title', 'artist_id', 'year', 'duration']) \
                    .dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = df_song.select(col('artist_id'),
                            col('artist_name').alias('name'),
                            col('artist_location').alias('location'),
                            col('artist_latitude').alias('latitude'),
                            col('artist_longitude').alias('longitude')) \
                    .dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    '''
    Function reads log file data elements and writes to parquet file.
    '''
    # get filepath to log data file
    # log_data = input_data + "log-data/*/*/*.json"     # S3
    log_data = input_data + "log-data/*.json"           # local

    # read log data file
    df_log = spark.read.json(log_data)

    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table
    users_table = df_log.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            col('gender'),
                            col('level') ) \
                    .dropDuplicates(['user_id'])

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users.parquet')


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x / 1000)) \
                                          .strftime('%Y-%m-%d %H:%M:%S'))

    df_log = df_log.withColumn("timestamp", to_timestamp(get_timestamp(df_log.ts)))

    # create datetime column from original timestamp column
    df_log = df_log.withColumn('start_time', F.from_unixtime(F.col('ts')/1000))

    time_table = df_log.select('ts', 'start_time') \
                   .withColumn('year', F.year('start_time')) \
                   .withColumn('month', F.month('start_time')) \
                   .withColumn('week', F.weekofyear('start_time')) \
                   .withColumn('weekday', F.dayofweek('start_time')) \
                   .withColumn('day', F.dayofyear('start_time')) \
                   .withColumn('hour', F.hour('start_time')).dropDuplicates(['ts'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    df_song = spark.read.json(input_data + "song_data/*/*/*/*.json")


    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df_log.join( df_song, (df_log.song == df_song.title) & \
                               (df_log.artist == df_song.artist_name),
                               'left_outer').select( col("timestamp").alias("start_time"),
                               col("userId").alias("user_id"),
                               df_log.level,
                               df_song.song_id,
                               df_song.artist_id,
                               col("sessionId").alias("session_id"),
                               df_log.location,
                               col("useragent").alias("user_agent"),
                               year("timestamp").alias("year"),
                               month("timestamp").alias("month") )


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays.parquet')


def main():
    '''
    Main function creates connection to spark cluster and calls functions to
    read JSON files and write data to parquet files.
    '''
    spark = create_spark_session()
    #input_data = "s3a://dend4-input/"
    #output_data = "s3a://dend4-output/"

    # in workspace with sample dataset
    input_data = "./data/"
    output_data = "./data/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
