# ============================================
# SQL Scripts to Create Tables in Redshift
# Author: <Your Name>
# ============================================

# ---------- Artists Table ----------
CREATE_ARTISTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.artists (
    artist_id      VARCHAR(256) PRIMARY KEY,
    artist_name    VARCHAR(256),
    artist_location VARCHAR(256),
    artist_latitude NUMERIC(18,0),
    artist_longitude NUMERIC(18,0)
);
"""

# ---------- Songplays Fact Table ----------
CREATE_SONGPLAYS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.songplays (
    play_id       VARCHAR(32) PRIMARY KEY,
    start_time    TIMESTAMP NOT NULL,
    user_id       INT NOT NULL,
    level         VARCHAR(50),
    song_id       VARCHAR(256),
    artist_id     VARCHAR(256),
    session_id    INT,
    location      VARCHAR(256),
    user_agent    VARCHAR(256)
);
"""

# ---------- Songs Dimension Table ----------
CREATE_SONGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.songs (
    song_id     VARCHAR(256) PRIMARY KEY,
    title       VARCHAR(256),
    artist_id   VARCHAR(256),
    release_year INT,
    duration    NUMERIC(18,0)
);
"""

# ---------- Time Dimension Table ----------
CREATE_TIME_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.time (
    start_time TIMESTAMP PRIMARY KEY,
    hour       INT,
    day        INT,
    week       INT,
    month      INT,
    year       INT,
    weekday    VARCHAR(50)
);
"""

# ---------- Users Dimension Table ----------
CREATE_USERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.users (
    user_id     INT PRIMARY KEY,
    first_name  VARCHAR(256),
    last_name   VARCHAR(256),
    gender      VARCHAR(10),
    level       VARCHAR(50)
);
"""

# ---------- Staging Events Table ----------
CREATE_STAGING_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.staging_events (
    artist          VARCHAR(256),
    auth            VARCHAR(50),
    first_name      VARCHAR(256),
    gender          VARCHAR(10),
    item_in_session INT,
    last_name       VARCHAR(256),
    song_length     NUMERIC(18,0),
    user_level      VARCHAR(50),
    location        VARCHAR(256),
    method          VARCHAR(10),
    page            VARCHAR(50),
    registration    NUMERIC(18,0),
    session_id      INT,
    song_name       VARCHAR(256),
    status          INT,
    timestamp_ms    BIGINT,
    user_agent      VARCHAR(256),
    user_id         INT
);
"""

# ---------- Staging Songs Table ----------
CREATE_STAGING_SONGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.staging_songs (
    num_songs         INT,
    artist_id         VARCHAR(256),
    artist_name       VARCHAR(256),
    artist_latitude   NUMERIC(18,0),
    artist_longitude  NUMERIC(18,0),
    artist_location   VARCHAR(256),
    song_id           VARCHAR(256),
    song_title        VARCHAR(256),
    duration          NUMERIC(18,0),
    release_year      INT
);
"""