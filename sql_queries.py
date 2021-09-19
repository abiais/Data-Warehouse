import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
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
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    id                  text,
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
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_id          int IDENTITY(0,1) PRIMARY KEY sortkey, 
    start_time           timestamp without time zone NOT NULL, 
    user_id              int NOT NULL, 
    level                text, 
    song_id              text, 
    artist_id            text, 
    session_id           int, 
    location             text, 
    user_agent           text 
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user (
    user_id             int PRIMARY KEY distkey, 
    first_name          text, 
    last_name           text, 
    gender              text, 
    level               text
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song (
    song_id             text PRIMARY KEY sortkey, 
    title               text, 
    artist_id           text, 
    year                int, 
    duration            numeric 
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id           text PRIMARY KEY sortkey, 
    name                text, 
    location            text, 
    latitude            numeric, 
    longitude           numeric
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    start_time          timestamp without time zone PRIMARY KEY sortkey distkey, 
    hour                int, 
    day                 int, 
    week                int, 
    month               int, 
    year                int, 
    weekday             int
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
    FROM {}
    iam_role '{}'
    REGION 'us-west-2'
    JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs
    FROM {}
    iam_role '{}'
    REGION 'us-west-2'
    JSON 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay
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
SELECT 
    DISTINCT 
    to_timestamp(to_char(staging_events.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
    staging_events.userId AS user_id,
    staging_events.level AS level,
    staging_events.song AS song_id,
    staging_songs.artist_id AS artist_id,
    staging_events.sessionId AS session_id,
    staging_events.location AS location,
    staging_events.userAgent AS user_agent
FROM 
    staging_events 
INNER JOIN 
    staging_songs
    ON staging_events.song = staging_songs.title 
    AND staging_events.artist = staging_songs.artist_name
WHERE
    staging_events.ts IS NOT NULL
    AND staging_events.userId IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO dim_user
(
    user_id,
    first_name,
    last_name,
    gender,
    level
) 
SELECT
    DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM
    staging_events
WHERE
    userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dim_song
(
    song_id, 
    title,
    artist_id,
    year,
    duration
) 
SELECT 
    DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM
    staging_songs
WHERE
    song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO dim_artist
(
    artist_id,
    name,
    location,
    latitude,
    longitude
) 
SELECT
    DISTINCT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM
    staging_songs
WHERE
    artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO dim_time
(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
) 
SELECT
    DISTINCT
    ts AS start_time,
    EXTRACT(hour FROM ts) AS hour,
    EXTRACT(day FROM ts) AS day, 
    EXTRACT(week FROM ts) AS week, 
    EXTRACT(month FROM ts) AS month, 
    EXTRACT(year FROM ts) AS year, 
    EXTRACT(weekday FROM ts) AS weekday
FROM
    staging_events
WHERE
    ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
