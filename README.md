# Airflow Final Exam
This is a re-sumbission of the Final Exam, Airflow course. In a nutshell, this project requires the use of Airflow to create a ETL pipeline (and by that I mean loading nested JSON into tables). 

## Project process
At a high-level, this project can be visualized throught the following diagram:
<img width="2141" height="511" alt="ProcessDiagram drawio" src="https://github.com/user-attachments/assets/ee0ae6e4-dc4a-49af-b871-86ccf15bc1ec" />

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
During testing, two dimension tables failed with UniqueViolation errors, which required more advanced SQL logic to resolve.

users Table: The source staging_events table can contain multiple records for the same userid if a user changes their subscription level (e.g., from 'free' to 'paid'). A simple SELECT DISTINCT is not sufficient.

Solution: The query was rewritten to use a ROW_NUMBER() window function, partitioned by userid and ordered by the event timestamp (ts). This allows us to select only the most recent record for each user, guaranteeing a unique entry.

time Table: The source songplays table can contain multiple song play events that occur at the exact same timestamp. Since start_time is the primary key of the time table, this caused a unique constraint violation.

Solution: The query was modified to select from a subquery that gets only the DISTINCT start_time values from the songplays table, ensuring that each timestamp is inserted only once.
