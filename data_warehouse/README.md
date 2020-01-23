# Data Warehouse - Udacity Data Engineer Nanodegree Project 3


## Overview
Project 3 of the Udacity Data Engineer Nanodegree involves the creation of a data warehouse for a music streaming startup, Sparkify.  Sparkify's
data resides in an AWS S3 bucket.  In this location the data is stored as JSON logs on user app activity and metadata on the app's songs.

It is the job of the data engineer to build an ETL pipeline that will extract the JSON data from S3 and stage them in intermediate tables in a RedShift 
database in AWS.  This staged data will then be transformed and loaded into a star schema data warehouse of fact and dimensional tables.  Those tables can
then be used by the analytics team to create insights for the business units to digest.  

## Sparkify Database in Redshift
The Sparkify database is housed as a Redshift cluster in AWS.  The database has a total of seven tables.  Two tables are used to stage the S3 JSON data and five are
used as analytical tables that are populated from the stage tables.  The fact tables in the Sparkify database form a star designed schema.  This schema design has four dimension tables and a single fact table.  

### Staging tables
The staging tables consist of two tables, staging_events and staging_songs.  They are described as follows:
*  staging_events: Data created from an event simulator based on songs in the dataset found in the staging_songs table.  The data simulates app activity
   from the Sparkify music streaming app.
*  staging_songs: This data is real data from the Million Song Dataset.  The data contains metadata about a song and the artist of that song.

### Fact Table
The fact table, songplays, records event data associated with song plays.  The table contains the following columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

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
The project requires a Redshift cluster to be created in AWS that the Python scripts interact with.  The Python scripts are all written using Python 3.#

### Create Redshift Cluster
The iPython Notebook lauch_redshift.ipynb steps through the process of creating the RedShift cluster and associated roles via infrastructure as code.  The following steps are required to build the Redshift cluster unless one has previously been provisioned.
*  Modify the file dwh-proj3.cfg.  This file contains the cofiguration information. 
   -  In the AWS section of the file place the key and secret values for the controling AWS account that will create the cluster.
*  Run launch_redshift.ipynb and step through the fields and follow the instructions in the file.  In section 2.2, you can acquire the endpoint & role ARN values that can be used within the dwh.cfg file. 
*  When done using the cluster you can follow the steps in the file to delete the created role and cluster.

NOTE: This file was created from a modified version of the Exercise 2 assignment in the Data Engineer Nanodegree lecture.

### Configure the dwh.cfg File
The dwh.cfg file needs to be modified to values corrisponding to the output of the launch_redshift.ipynb file or those of the prexisitng provisioned Redshift cluster.  The host value and ARN can come from a prexisiting Redshift cluster or the one created by the launch_redshift.ipynb file. The other values in the dwh.cfg file have been prefilled; however, those values can be adjusted as the user sees fit if they DO NOT use the launch_redshift.ipynb file.   

### Run create_tables.py
Launch a terminal and run the following command:
> python create_tables.py

This will first drop any existing tables and create the necessary tables in the Redshift cluster.  Make sure you have completed the conifguration and have an active Redshift cluster prior to running this file.

### Run etl.py
Launch a terminal and run the following command:
> python etl.py

This will first copy the data from the S3 bucket into the staging tables.  It will then transform that data and insert into the analytical tables.  Make sure you have completed the conifguration and have an active Redshift cluster prior to running this file and run the create_tables.py script.
