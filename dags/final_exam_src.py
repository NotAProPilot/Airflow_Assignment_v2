# Import necessary libraries
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowException
import json
import os

"""The insert statements alongside the file. 

Please note that I have made the following changes to the insert statements:

1. Add `TRUNCATE TABLE` before adding any data. I found out the hard way that this table has a lot of 'unique' constraint,
so just to be safe, I have add a lot of truncate statement to de-d
"""
class SqlQueries:
    songplay_table_insert = ("""
        TRUNCATE TABLE songplays;
        INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT
                md5(CAST(events.sessionid AS TEXT) || CAST(events.start_time AS TEXT)) as playid,
                events.start_time, 
                events.userid, 
                events."level", 
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
                AND events.length = songs.duration;
    """)

    user_table_insert = ("""
        TRUNCATE TABLE users;
        INSERT INTO users (userid, first_name, last_name, gender, "level")
        SELECT userid, firstname, lastname, gender, "level"
        FROM (
            SELECT 
                userid, 
                firstname, 
                lastname, 
                gender, 
                "level",
                ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) as rn
            FROM staging_events
            WHERE page = 'NextSong' AND userid IS NOT NULL
        ) tmp
        WHERE rn = 1;
    """)

    song_table_insert = ("""
        TRUNCATE TABLE songs;
        INSERT INTO songs (songid, title, artistid, "year", duration)
        SELECT distinct song_id, title, artist_id, "year", duration
        FROM staging_songs;
    """)

    artist_table_insert = ("""
        TRUNCATE TABLE artists;
        INSERT INTO artists (artistid, name, location, lattitude, longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
    """)

    time_table_insert = ("""
        TRUNCATE TABLE "time";
        INSERT INTO "time" (start_time, "hour", "day", week, "month", "year", weekday)
        SELECT 
            start_time, 
            extract(hour from start_time), 
            extract(day from start_time), 
            extract(week from start_time), 
            extract(month from start_time), 
            extract(year from start_time), 
            extract(dow from start_time)
        FROM (SELECT DISTINCT start_time FROM songplays) AS unique_songplays;
    """)

def load_songs_into_table(**kwargs):
    """Loads song data from JSON files into the staging_songs table."""
    
    # Connection:
    postgres_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'])
    data_path = "/opt/airflow/data/song_data/"
    
    # Truncate data (since data has already been loaded)
    postgres_hook.run("TRUNCATE TABLE public.staging_songs;")
    
    # Insert SQL statement
    insert_sql = """
    INSERT INTO public.staging_songs (
        num_songs, artist_id, artist_name, artist_latitude, artist_longitude, artist_location, song_id, title, duration, "year"
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    
    # Looping thru all files
    for root, _, files in os.walk(data_path):
        for file in files:
            if file.endswith('.json'):
                filepath = os.path.join(root, file)
                with open(filepath, 'r') as song_file:
                    song_data = json.load(song_file)
                    postgres_hook.run(insert_sql, parameters=(
                        song_data.get('num_songs'), song_data.get('artist_id'), song_data.get('artist_name'),
                        song_data.get('artist_latitude'), song_data.get('artist_longitude'), song_data.get('artist_location'),
                        song_data.get('song_id'), song_data.get('title'), song_data.get('duration'), song_data.get('year')
                    ))

def load_logs_into_table(**kwargs):
    """Loads log data from JSON files into the staging_events table."""
    postgres_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'])
    data_path = "/opt/airflow/data/log_data/"
    
    postgres_hook.run("TRUNCATE TABLE public.staging_events;")
    
    insert_sql = """
    INSERT INTO public.staging_events (
        artist, auth, firstname, gender, iteminsession, lastname, length, "level", location, method, page, 
        registration, sessionid, song, status, ts, useragent, userid
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    
    for root, _, files in os.walk(data_path):
        for file in files:
            if file.endswith('.json'):
                filepath = os.path.join(root, file)
                with open(filepath, 'r') as log_file:
                    for line in log_file:
                        log_data = json.loads(line)

                        # --- DATA CLEANING STEP ---
                        # If userId is an empty string, change it to None before inserting.
                        user_id = log_data.get('userId')
                        if user_id == '':
                            user_id = None
                        # --------------------------
                        
                        postgres_hook.run(insert_sql, parameters=(
                            log_data.get('artist'), log_data.get('auth'), log_data.get('firstName'), log_data.get('gender'),
                            log_data.get('itemInSession'), log_data.get('lastName'), log_data.get('length'), log_data.get('level'),
                            log_data.get('location'), log_data.get('method'), log_data.get('page'), log_data.get('registration'),
                            log_data.get('sessionId'), log_data.get('song'), log_data.get('status'), log_data.get('ts'),
                            log_data.get('userAgent'), 
                            user_id # Use the cleaned user_id variable here
                        ))

def data_quality_check(**kwargs):
    """Performs data quality checks on the final tables."""
    postgres_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'])
    tables = ['songplays', 'users', 'artists', 'time', 'songs']
    
    for table in tables:
        records = postgres_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        
        if not records or not records[0] or records[0][0] < 1:
            raise AirflowException(f"Data quality check failed for table {table}. No records found.")
        
        print(f"Data quality check passed for {table} with {records[0][0]} records.")

# --- DAG Definition ---
## TODO: MODIFY THIS TO TEST DAG
default_args = {
    'owner': 'QuangBLM1',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 24),
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'email_on_retry': False,
    'catchup': False,
}

with DAG(
    dag_id='tunestream_etl_pipeline',
    default_args=default_args,
    description='Load and transform data in PostgreSQL with Airflow',
    schedule_interval='@hourly'
) as dag:

    begin_execution = EmptyOperator(task_id='Begin_execution')
    
    stage_songs = PythonOperator(
        task_id='Stage_songs',
        python_callable=load_songs_into_table,
        op_kwargs={'postgres_conn_id': 'postgres_default'}
    )

    stage_events = PythonOperator(
        task_id='Stage_events',
        python_callable=load_logs_into_table,
        op_kwargs={'postgres_conn_id': 'postgres_default'}
    )

    load_songplay_fact_table = PostgresOperator(
        task_id = 'Load_songplays_fact_table',
        postgres_conn_id = 'postgres_default',
        sql=SqlQueries.songplay_table_insert,
    )

    load_song_dim_table = PostgresOperator(task_id = 'Load_song_dim_table', postgres_conn_id = 'postgres_default', sql=SqlQueries.song_table_insert)
    load_user_dim_table = PostgresOperator(task_id = 'Load_user_dim_table', postgres_conn_id = 'postgres_default', sql=SqlQueries.user_table_insert)
    load_artist_dim_table = PostgresOperator(task_id = 'Load_artist_dim_table', postgres_conn_id = 'postgres_default', sql=SqlQueries.artist_table_insert)
    load_time_dim_table = PostgresOperator(task_id = 'Load_time_dim_table', postgres_conn_id = 'postgres_default', sql=SqlQueries.time_table_insert)
    
    check_data_quality = PythonOperator(
        task_id='Run_data_quality_checks',
        python_callable=data_quality_check,
        op_kwargs={'postgres_conn_id': 'postgres_default'}
    )
    
    end_execution = EmptyOperator(task_id='End_execution')
    
    # --- Task Dependencies ---
    begin_execution >> [stage_songs, stage_events]
    [stage_songs, stage_events] >> load_songplay_fact_table
    load_songplay_fact_table >> [load_artist_dim_table, load_song_dim_table, load_time_dim_table, load_user_dim_table]
    [load_artist_dim_table, load_song_dim_table, load_time_dim_table, load_user_dim_table] >> check_data_quality
    check_data_quality >> end_execution