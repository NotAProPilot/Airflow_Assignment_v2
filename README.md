# Airflow Final Exam
This is a re-sumbission of the Final Exam, Airflow course. In a nutshell, this project requires the use of Airflow to create a ETL pipeline (and by that I mean loading nested JSON into tables). 

## Project process
At a high-level, this project can be visualized throught the following diagram:
<img width="2141" height="511" alt="ProcessDiagram drawio" src="https://github.com/user-attachments/assets/ee0ae6e4-dc4a-49af-b871-86ccf15bc1ec" />

## Folder structure
```md
tunestream_etl_pipeline/
│
├── dags/
│   └── tunestream_etl_pipeline.py  # The main Airflow DAG file.
│
├── data/
│   ├── song_data/                  # Contains song JSON files.
│   └── log_data/                   # Contains event log JSON files.
│
├── sql/
│   └── create_tables.sql           # SQL script to create the database schema.
│
├── .env                            # Environment variables for Docker.
└── docker-compose.yaml             # Defines the Docker services (Airflow, Postgres, etc.).
```



## 0. Installing Docker and Airflow
Follow installation guides for Docker. After that, installing Airflow on Docker. If you are on Linux on Mac, you can skip this step and just install Airflow + Postgres locally. 

***Important***:
1. Do NOT use Airflow 3.0 for this project. If you use Airflow 3.0, the `PostgresOperator` will NOT work. (Speaking from personal experience 😅). You WILL need that to actually load data into table.
2. Do NOT install Postgres on Docker. Airflow on Docker comes with Postgres, so there's no need to setup anything extra. Running Postgres on Docker WILL break your connection to the database (beacause the Airflow container won't connect to the Postgres container). 

## 1. Modify SQL statements
To ensure the pipeline is robust and produces consistent results, several modifications were made to the SQL INSERT statements to handle data duplication and unique constraint violations.

### 1.1: Adding TRUNCATE for Idempotency
The initial `INSERT` queries would append data on every DAG run, leading to massive duplication. To fix this, the pipeline was made idempotent by clearing tables before loading new data. Because of that, a `TRUNCATE TABLE public.staging_...;` command was added before loading data in functions for all tables.

### 1.2: Unique Constraint Compliance
During testing, two dimension tables failed with UniqueViolation errors, which required ✨some✨ creativity to solve. 

| Table | Problem | Solution |
| :---- | :--- | :--- |
| `users` | The source `staging_events` table can contain multiple records for the same `userid` if a user changes their subscription level (e.g., from 'free' to 'paid'). A simple `SELECT DISTINCT` is not sufficient. | The query was rewritten to use a `ROW_NUMBER()` window function, partitioned by `userid` and ordered by the event timestamp (`ts`). This allows us to select only the most recent record for each user, guaranteeing a unique entry. |
| `time` | The source `songplays` table can contain multiple song play events that occur at the exact same timestamp. Since `start_time` is the primary key of the `time` table, this caused a unique constraint violation. | The query was modified to select from a subquery that gets only the `DISTINCT start_time` values from the `songplays` table, ensuring that each timestamp is inserted only once. |

## 2. Creaing table
Now, assuming you are in the root directory (which contains `dag` and so on), do the following steps:

1. Move the provided `create_tables.sql` into a folder named `sql`.
2. In the root dir, open Powershell/CMD, and type in `docker exec -i (Container ID of built-in Airflow Postgres) psql -U airflow -d airflow < sql\create_tables.sql`
3. If you see 6 `CREATE TABLE` statements in the CLI, congrats, process is completed.

### How to find Container ID?
Type `docker ps`. It'll shows something like this:
<img width="1811" height="179" alt="image" src="https://github.com/user-attachments/assets/f697176f-7303-435f-944a-adf5d9cd4140" />

Find the ID for `postgres:13`. 

## 3. Writing the DAG
At a high-level, the pseudocode for the entire DAG is like this:
```python

# The class SQLQuries provided:
clas SQLQueries:

  # Code for inserting into songplay table:
  songplay_table_insert = (
    """
      INSERT INTO ....
    """
  )

  # The same for other tables

# Function to load data into STAGING song table:
def load_songs_into_staging_table(**kwargs):
    # Establish a connection to the database using the provided connection ID
    db_hook = Connect_to_Postgres(connection_id = kwargs['postgres_conn_id'])
    
    # Define the source directory for the song data files
    song_data_directory = "/opt/airflow/data/song_data/"
    
    # Ensure idempotency by clearing the staging table before loading
    db_hook.execute("TRUNCATE TABLE public.staging_songs")
    
    # Prepare the parameterized SQL INSERT statement
    insert_statement = "INSERT INTO public.staging_songs VALUES (%s, %s, ...)"
    
    # Recursively find and process every JSON file in the source directory
    for each_file in find_all_files(song_data_directory, ending_in=".json"):
        
        # Read the file and parse its contents as a JSON object
        song_data = parse_json(file)
        
        # Extract values from the JSON object in the correct order for insertion
        record_parameters = (
            song_data['num_songs'],
            song_data['artist_id'],
            ...
            song_data['year']
        )
        
        # Execute the INSERT statement, passing the extracted parameters
        db_hook.execute(insert_statement, parameters=record_parameters)

# Simillarly for loading LOG into STAGING TABLE:
def load_logs_into_staging_table():
  # Similar logics...
```
