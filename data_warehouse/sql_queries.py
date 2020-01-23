import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA     = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA    = config.get('S3', 'SONG_DATA')
ARN          = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
  artist        VARCHAR,
  auth          VARCHAR,
  firstName     VARCHAR,
  gender        VARCHAR,
  itemInSession VARCHAR,
  lastName      VARCHAR,
  length        VARCHAR,
  level         VARCHAR,
  location      VARCHAR,
  method        VARCHAR,
  page          VARCHAR,
  registration  VARCHAR,
  sessionId     INTEGER NOT NULL SORTKEY DISTKEY,
  song          VARCHAR,
  status        INTEGER,
  ts            NUMERIC,
  userAgent     VARCHAR,
  userId        INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
  num_songs        INTEGER,
  artist_id        VARCHAR NOT NULL SORTKEY DISTKEY,
  artist_latitude  VARCHAR,
  artist_longitude VARCHAR,
  artist_location  VARCHAR,
  artist_name      VARCHAR,
  song_id          VARCHAR NOT NULL,
  title            VARCHAR,
  duration         NUMERIC,
  year             INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
  songplay_id     INTEGER IDENTITY(0,1) PRIMARY KEY SORTKEY,
  start_time      TIMESTAMP NOT NULL,
  user_id         INTEGER NOT NULL DISTKEY,
  level           VARCHAR NOT NULL,
  song_id         VARCHAR NOT NULL,
  artist_id       VARCHAR NOT NULL,
  session_id      VARCHAR NOT NULL,
  location        VARCHAR ,
  user_agent      VARCHAR 
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
  user_id    INTEGER PRIMARY KEY SORTKEY,
  first_name VARCHAR NOT NULL,
  last_name  VARCHAR NOT NULL,
  gender     CHAR(1) NOT NULL,
  level      VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
  song_id   VARCHAR PRIMARY KEY SORTKEY,
  title     VARCHAR NOT NULL,
  artist_id VARCHAR NOT NULL,
  year      INTEGER NOT NULL,
  duration  NUMERIC NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
  artist_id VARCHAR PRIMARY KEY SORTKEY,
  name      VARCHAR,
  location  VARCHAR,
  latitude  NUMERIC,
  longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times 
(
  start_time TIMESTAMP PRIMARY KEY SORTKEY,
  hour       SMALLINT NOT NULL,
  day        SMALLINT NOT NULL,
  week       SMALLINT NOT NULL,
  month      SMALLINT NOT NULL,
  year       SMALLINT NOT NULL,
  weekday    SMALLINT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, ARN)


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, 
                           artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second',
           se.userId,
           se.level,
           ss.song_id,
           ss.artist_id,
           se.sessionId,
           se.location,
           se.userAgent
    FROM staging_events se
        JOIN staging_songs ss ON (se.artist = ss.artist_name AND se.song = ss.title)
    WHERE se.page = 'NextSong';
           
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT se.userId, se.firstName, se.lastName, se.gender, se.level
    FROM staging_events se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
           EXTRACT(hour FROM start_time),
           EXTRACT(day FROM start_time),
           EXTRACT(week FROM start_time),
           EXTRACT(month FROM start_time),
           EXTRACT(year FROM start_time),
           EXTRACT(week FROM start_time)
    FROM staging_events
    WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
