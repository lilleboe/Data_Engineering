class SqlQueries:
    
    staging_events_table_create = ("""
        DROP TABLE IF EXISTS public.staging_events;
        CREATE TABLE IF NOT EXISTS public.staging_events (
            artist varchar(256),
            auth varchar(256),
            firstname varchar(256),
            gender varchar(256),
            iteminsession int4,
            lastname varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionid int4,
            song varchar(256),
            status int4,
            ts int8,
            useragent varchar(256),
            userid int4
        );"""
                                  )

    staging_songs_table_create = ("""
        DROP TABLE IF EXISTS public.staging_songs;
        CREATE TABLE IF NOT EXISTS public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        );"""
                                 )

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS public.songplays (
            playid bigint IDENTITY(0,1) PRIMARY KEY,
            start_time timestamp NOT NULL,
            userid int4 NOT NULL,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256)
        );"""
                            )

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS public.users (
            userid int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        );"""
                        )

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS public.songs (
            songid varchar(256) NOT NULL,
            title varchar(256),
            artistid varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );"""
                        )

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS public.artists (
            artistid varchar(256) NOT NULL,
            name varchar(256),
            location varchar(256),
            latitude numeric(18,0),
            longitude numeric(18,0)
        );"""
                          )

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS public.times 
        (
          start_time TIMESTAMP PRIMARY KEY SORTKEY,
          hour       SMALLINT NOT NULL,
          day        SMALLINT NOT NULL,
          week       SMALLINT NOT NULL,
          month      SMALLINT NOT NULL,
          year       SMALLINT NOT NULL,
          weekday    SMALLINT NOT NULL
        );"""
                        )

    songplay_table_insert = ("""
        INSERT INTO songplays (
                start_time,
                userid,
                level,
                songid,
                artistid,
                sessionid,
                location,
                user_agent
        ) 
        SELECT
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
            FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO users (
                userid,
                first_name,
                last_name,
                gender,
                level
        ) 
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs (
                songid,
                title,
                artistid,
                year,
                duration
        ) 
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists (
                artistid,
                name,
                location,
                latitude,
                longitude
        ) 
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO times (
                start_time,
                hour,
                day,
                week,
                month,
                year,
                weekday
        ) 
        SELECT  start_time, 
                extract(hour from start_time) as hour, 
                extract(day from start_time) as day, 
                extract(week from start_time) as week, 
                extract(month from start_time) as month, 
                extract(year from start_time) as year, 
                extract(dayofweek from start_time) as weekday
        FROM songplays
    """)
    
    data_quality_count_test = ("""
        SELECT COUNT(*) FROM {}
    """)