# Data Lake - Udacity Data Engineer Nanodegree Project 4


## Overview
Project 4 of the Udacity Data Engineer Nanodegree involves the creation of data marts for a music streaming startup, Sparkify.  Sparkify's
data resides in an AWS S3 bucket.  In this location the data is stored as JSON logs on user app activity and metadata on the app's songs.

It is the job of the data engineer to build an ETL pipeline that will extract the JSON data from S3 and process them with Spark.  This staged data 
will then be transformed and loaded into a star schema of fact and dimensional tables back into S3.  Those tables can
then be used by the analytics team to create insights for the business units to digest.  

### Fact Table
The fact table, songplays, records event data associated with song plays.  
The table contains the following columns: 
* columns: songplay_id, start_time, year, month, user_id, level, song_id, artist_id, session_id, location, user_agent

It should be noted that the original specs did not specify a year and month column; however, the template provided requires the data to be
partitioned by year and month.  Those extra columns were added to accomidate this requirement.


### Dimension Tables
There are four supporting dimension tables that are described as follows:
*  users: users in the app
   - columns: user_id, first_name, last_name, gender, level
*  songs: songs in music database
   -  columns: song_id, title, artist_id, year, duration
*  artists: artists in music database
   -  columns: artist_id, name, location, lattitude, longitude
*  time: timestamps of records in songplays broken down into specific units
   -  columns: start_time, hour, day, week, month, year, weekday


## Running the Project
The project requires a S3 bucket to be created in AWS.  The Python scripts are all written using Python 3.#

### Configure the dwh.cfg File
The dl.cfg file needs to be modified.  The following lines need modification:
* AWS_ACCESS_KEY_ID     = \<YOUR ACCESS KEY\>
* AWS_SECRET_ACCESS_KEY = \<YOUR SECRET ACCESS KEY\>
* OUTPUT_DATA           = s3a://\<YOUR S3 BUCKET\>/
    
The following values may or may not require modification depending if you plan to pull from Udacity's S3 buckets
* INPUT_DATA       = s3a://udacity-dend/
* SONG_DATA        = s3a://udacity-dend/song_data/\*/\*/\*/\*.json
* LOG_DATA         = s3a://udacity-dend/log_data/\*/\*/\*.json
* SINGLE_SONG_DATA = s3a://udacity-dend/song_data/A/A/A/\*.json
* SINGLE_LOG_DATA  = s3a://udacity-dend/log_data/2018/11/2018-11-01-events.json

### Run etl.py
Launch a terminal and run the following command:
> python etl.py

This will first load the data from the S3 bucket into Spark.  It will then transform that data and insert into the analytical tables stored in another S3 instance.  Make sure you have completed the conifguration and have an active S3 (or local location) prior to running this python script.