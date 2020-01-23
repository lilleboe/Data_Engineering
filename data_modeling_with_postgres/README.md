## Udacity Data Engineering Nanodegree Project 1

#### Project Summary
In project 1 data modeling with Postgres and building an ETL pipeline using Python are the core focus. Fact and dimension tables for a star schema were defined and created for the project.  An ETL pipeline that reads in data from JSON song and log files was created to transfer data to tables in the Postgres database.  The results of the project are to help Sparkify query for user usage metrics in order to potentially help answer questions such as:
- How many users are logging into the application?
- What songs are users listening to?
- What type of agents are users utilizing to access music?

#### How to run the scripts
Run on a command line the create_tables.py file followed by the etl.py file in order to first create the tables in the database then run the ETL code.

#### File descriptions
- create_tables.py
    - Creates all the tables by first dropping them if they exist
- etl.py
    - Contains the essential ETL code to extract data from the JSON files and populate the database
- sql_quiries.py
    - The various queries to drop, create and insert tables.  It also contains any other queries to extract data from the database
- etl.ipynb
    - Python notebook that has instrucions on how to create the ETL code that was used as a foundation for the etl.py file
- test.ipynb
    - Python notebook that is used to test if data is being inserted into the database tables
- README.md
    - Readme file used that provides a summary of the project
    
#### Database schema
The database consisted of 5 tables:
- songplays
    - No primary key.
- users
    - 
- songs
- artists
- time

#### ETL pipeline
The ETL pipline didn't divert from the predefined expectations that were detailed in the project's description.  