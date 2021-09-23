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
    artist              varchar,
    auth                varchar,
    firstName           varchar,
    gender              varchar(1),
    itemInSession       int,
    lastName            varchar,
    length              numeric,
    level               varchar,
    location            varchar,
    method              varchar(3),
    page                varchar,
    registration        varchar,
    sessionId           int,
    song                varchar,
    status              int,
    ts                  bigint,
    userAgent           varchar,
    userId              int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           int,
    artist_id           varchar,
    artist_latitude     decimal,
    artist_longitude    decimal, 
    artist_location     varchar,
    artist_name         varchar,
    song_id             varchar,
    title               varchar,
    duration            numeric,
    year                int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
    id                   int IDENTITY(0,1) PRIMARY KEY sortkey, 
    start_time           timestamp without time zone NOT NULL, 
    user_id              int NOT NULL, 
    level                varchar, 
    song_id              varchar, 
    artist_id            varchar, 
    session_id           int, 
    location             varchar, 
    user_agent           varchar 
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user (
    id                  int PRIMARY KEY sortkey, 
    first_name          varchar, 
    last_name           varchar, 
    gender              varchar, 
    level               varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song (
    id                  varchar PRIMARY KEY sortkey, 
    title               varchar, 
    artist_id           varchar, 
    year                int, 
    duration            numeric 
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist (
    id                  varchar PRIMARY KEY sortkey, 
    name                varchar, 
    location            varchar, 
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
    TIMESTAMP 'epoch' + staging_events.ts/1000 *INTERVAL '1 second' AS start_time,
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
    AND staging_events.length = staging_songs.duration
WHERE
    staging_events.ts IS NOT NULL
    AND staging_events.userId IS NOT NULL
    AND staging_events.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO dim_user
(
    id,
    first_name,
    last_name,
    gender,
    level
) 
SELECT
    DISTINCT
    userId AS id,
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
    id, 
    title,
    artist_id,
    year,
    duration
) 
SELECT 
    DISTINCT
    song_id AS id,
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
    id,
    name,
    location,
    latitude,
    longitude
) 
SELECT
    DISTINCT
    artist_id AS id,
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
    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS hour,
    EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS day, 
    EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS week, 
    EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS month, 
    EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS year, 
    EXTRACT(weekday FROM TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS weekday
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
