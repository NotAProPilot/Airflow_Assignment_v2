# Airflow Final Exam
This is a re-sumbission of the Final Exam, Airflow course. In a nutshell, this project requires the use of Airflow to create a ETL pipeline (and by that I mean loading nested JSON into tables). 

## Project process
At a high-level, this project can be visualized throught the following diagram:
<img width="2141" height="511" alt="ProcessDiagram drawio" src="https://github.com/user-attachments/assets/ee0ae6e4-dc4a-49af-b871-86ccf15bc1ec" />

## 0. Installing Docker and Airflow
Follow installation guides for Docker. After that, installing Airflow on Docker.

***Important***:
1. Do NOT use Airflow 3.0 for this project. If you use Airflow 3.0, the `PostgresOperator` will NOT work. (Speaking from personal experience 😅). You WILL need that to actually load data into table.
2. Do NOT install Postgres on Docker. Airflow on Docker comes with Postgres, so there's no need to setup anything extra. Running Postgres on Docker WILL break your connection to the database (beacause the Airflow container won't connect to the Postgres container). 
