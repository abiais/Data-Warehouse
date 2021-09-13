import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    id                  IDENTITY(0,1) NOT NULL PRIMARY KEY,
    artist              text,
    auth                text,
    firstName           text,
    gender              text,
    itemInSession       int,
    lastName            text,
    length              numeric,
    level               text,
    location            text,
    method              text,
    page                text,
    registration        numeric,
    sessionId           int,
    song                text,
    status              int,
    ts                  bigint,
    userAgent           text,
    userId              int
)
    
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    id                  IDENTITY(0,1) NOT NULL PRIMARY KEY,
    num_songs           int,
    artist_id           text,
    artist_latitude     text,
    artist_longitude    text, 
    artist_location     text,
    artist_name         text,
    song_id             text,
    title               text,
    duration            numeric,
    year                int
)
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id          IDENTITY(0,1) NOT NULL PRIMARY KEY, 
    start_time           timestamp without time zone NOT NULL, 
    user_id              int NOT NULL, 
    level                text, 
    song_id              text, 
    artist_id            text, 
    session_id           int, 
    location             text, 
    user_agent           text 
)""")

user_table_create = ("""
CREATE TABLE user (
    user_id             int NOT NULL PRIMARY KEY, 
    first_name          text, 
    last_name           text, 
    gender              text, 
    level               text
)""")

song_table_create = ("""
CREATE TABLE song (
    song_id             text NOT NULL PRIMARY KEY, 
    title               text, 
    artist_id           text, 
    year                int, 
    duration            numeric 
)""")

artist_table_create = ("""
CREATE TABLE artist (
    artist_id           text NOT NULL PRIMARY KEY, 
    name                text, 
    location            text, 
    latitude            numeric, 
    longitude           numeric
)""")

time_table_create = ("""
CREATE TABLE events (
    start_time          timestamp without time zone NOT NULL PRIMARY KEY, 
    hour                int, 
    day                 int, 
    week                int, 
    month               int, 
    year                int, 
    weekday             int
)""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
    FROM 's3://udacity-dend/log_data'
    iam_role '{}'
    REGION 'us-west-2'
    JSON 's3://udacity-dend/log_json_path.json';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
""").format()
staging_songs_copy = ("""
COPY staging_songs
    FROM {}
    iam_role {}
    REGION 'us-west-2'
    json 'auto';

""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays 
(
    songplay_id,
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
    VALUES 
(
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
)
""")

user_table_insert = ("""
INSERT INTO users 
(
    user_id,
    first_name,
    last_name,
    gender,
    level
) 
VALUES 
(
    %s,
    %s,
    %s,
    %s,
    %s
) 
ON CONFLICT 
    ON CONSTRAINT users_pkey DO UPDATE SET 
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        gender = EXCLUDED.gender,
        level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs 
(
    song_id, 
    title,
    artist_id,
    year,
    duration
) 
VALUES 
(
    %s,
    %s,
    %s,
    %s,
    %s
) 
ON CONFLICT ON CONSTRAINT 
    songs_pkey DO UPDATE SET 
        title = EXCLUDED.title,
        artist_id = EXCLUDED.artist_id,
        year = EXCLUDED.year,
        duration = EXCLUDED.duration
""")

artist_table_insert = ("""
INSERT INTO artists 
(
    artist_id,
    name,
    location,
    latitude,
    longitude
) 
VALUES 
(
    %s,
    %s,
    %s,
    %s,
    %s
    ) 
ON CONFLICT ON CONSTRAINT artists_pkey DO UPDATE SET 
    name = EXCLUDED.name,
    location = EXCLUDED.location,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude
""")

time_table_insert = ("""
INSERT INTO times 
(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
) 
VALUES 
(
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
) 
ON CONFLICT ON CONSTRAINT times_pkey DO NOTHING
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
