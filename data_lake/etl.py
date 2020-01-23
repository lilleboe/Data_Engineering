import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as pt


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ 
    Creates a spark session that is used to process data
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, song_input_data, output_data):
    """
    Processes song data that is stored as JSON files from the location
    in the input_data paramater.
    """
    # get filepath to song data file
    song_data = song_input_data
    
    # read song data file
    print("Processing JSON song data...")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    print("Extracting columns for song table...")
    df.createOrReplaceTempView("songs_table")
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songs_table
        ORDER BY song_id
    """) 
    
    print("Song table sample:")
    songs_table.show(5, truncate=False)
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs table to parquet files...")
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id")\
    .parquet(output_data + "songs_table.parquet")
    
    # extract columns to create artists table
    print("Extracting columns for artists table...")
    df.createOrReplaceTempView("artists_table")
    artists_table = spark.sql("""
        SELECT artist_id,
               artist_name      AS name,
               artist_location  AS location,
               artist_latitude  AS latitude,
               artist_longitude AS longitude
        FROM artists_table
        ORDER BY artist_id DESC
    """) 

    print("Artists table sample:")
    artists_table.show(5, truncate=False)
    
    # write artists table to parquet files
    print("Writing artists table to parquet files...")
    songs_table.write.mode("overwrite").parquet(output_data + "artists_table.parquet")
    

def process_log_data(spark, song_input_data, log_input_data, output_data):
    """
    Processes log data that is stored as JSON files.
    """
    # get filepath to log data file
    log_data = log_input_data

    # read log data file
    print("Reading in the log data...")
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    print("Filtering by actions for song plays...")
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    print("Extracting columns for users table...")
    log_df.createOrReplaceTempView("users_table")
    users_table = spark.sql("""
        SELECT  DISTINCT userId    AS user_id,
                         firstName AS first_name,
                         lastName  AS last_name,
                         gender,
                         level
        FROM users_table
        ORDER BY last_name
    """)
    
    print("Users table sample:")
    users_table.show(5, truncate=False)
    
    # write users table to parquet files
    print("Writing users table to parquet files...")
    users_table.write.mode("overwrite").parquet(output_data + "users_table.parquet")

    # create timestamp column from original timestamp column
    print("Creating timestamp column...")
    @udf(pt.TimestampType())
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)

    log_df = log_df.withColumn("timestamp", get_timestamp("ts"))
    log_df.show(5)
    
    # create datetime column from original timestamp column
    print("Creating datetime column...")
    @udf(pt.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    log_df = log_df.withColumn("datetime", get_datetime("ts"))
    log_df.show(5)
    
    # extract columns to create time table
    print("Extracting columns for time table...")
    log_df.createOrReplaceTempView("time_table")
    time_table = spark.sql("""
        SELECT  DISTINCT datetime              AS start_time,
                         hour(timestamp)       AS hour,
                         day(timestamp)        AS day,
                         weekofyear(timestamp) AS week,
                         month(timestamp)      AS month,
                         year(timestamp)       AS year,
                         dayofweek(timestamp)  AS weekday
        FROM time_table
        ORDER BY start_time
    """)
    
    print("Users table sample:")
    time_table.show(5, truncate=False)
    
    # write time table to parquet files partitioned by year and month
    print("Writing time table to parquet files...")
    time_table.write.mode("overwrite").partitionBy("year", "month")\
            .parquet(output_data + "time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(song_input_data)

    print("Joining log_data and song_data...")
    joined_df = log_df.join(song_df, (log_df.artist == song_df.artist_name) & (log_df.song == song_df.title))

    # extract columns from joined song and log datasets to create songplays table 
    print("Extracting columns from joined DF...")
    joined_df = joined_df.withColumn("songplay_id", monotonically_increasing_id())
    joined_df.createOrReplaceTempView("songplays_table")
    songplays_table = spark.sql("""
        SELECT  songplay_id,
                timestamp        AS start_time,
                month(timestamp) AS month,
                year(timestamp)  AS year,
                userId           AS user_id,
                level,
                song_id,
                artist_id,
                sessionId        AS session_id,
                location,
                userAgent        AS user_agent
        FROM songplays_table
        ORDER BY (user_id, session_id)
    """)

    print("Song plays table sample:")
    songplays_table.show(5, truncate=False)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month") \
    .parquet(output_data + "songplays_table.parquet")


def main():
    """
    Loads input data from JSON files
    """

    print("Initiating Spark session...")
    print('-' * 50)
    spark = create_spark_session()
    
    # Use these settings if you want to test on the full
    # dataset, but it takes a LONG time.
    song_input_data = config['AWS']['SONG_DATA']
    log_input_data = config['AWS']['LOG_DATA']
    
    # Uncomment the two lines if you want to test on
    # minimal data
    #song_input_data = config['AWS']['SINGLE_SONG_DATA']
    #log_input_data = config['AWS']['SINGLE_LOG_DATA']
    
    output_data = config['AWS']['OUTPUT_DATA']
    
    print('-' * 50)
    print("Processing song data...")
    print('-' * 50)
    print('')
    process_song_data(spark, song_input_data, output_data)
    
    print('-' * 50)    
    print("Processing log data...")
    print('-' * 50)
    print('')
    process_log_data(spark, song_input_data, log_input_data, output_data)


if __name__ == "__main__":
    main()
