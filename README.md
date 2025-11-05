# spark-etl

A Spark ETL pipeline for aggregating user settings events, orchestrated with Airflow and dag-factory.

- [Project structure](https://github.com/nataliaarhus/spark-etl/blob/main/README.md#project-structure)
- [Architecture](https://github.com/nataliaarhus/spark-etl/blob/main/README.md#architecture)
- [Setting up a local environment](https://github.com/nataliaarhus/spark-etl/blob/main/README.md#Setting-up-a-local-environment)
- [Running the DAG](https://github.com/nataliaarhus/spark-etl/blob/main/README.md#running-the-dag)
- [Running unit tests](https://github.com/nataliaarhus/spark-etl/blob/main/README.md#running-unit-tests)
- [Troubleshooting](https://github.com/nataliaarhus/spark-etl/blob/main/README.md#troubleshooting)

## Project structure
```
.
├── airflow
├── dags
│   ├── dag_configs
├── include
│   └── scripts
├── logs
├── res
├── tests
├── Dockerfile
├── LICENSE
├── README.md
├── docker-compose.yml
└── requirements.txt
```
- **dags** - the DAGs definitions using the Dag Factory from Astronomer. `dag_factory_loader.py` parses DAGs based on the YML definitions specified in the `dags_configs`.
- **include** - scripts invoked in the DAGs during execution. 
- **logs** - logs from the execution (gitignored).
- **res** - contains dummy data for program execution.
- **tests** - unit tests testing the transofrmation logic.

## Architecture


- **Orchestration**: Daily DAG via Airflow, configurable via YAML (dag-factory).
- **Infrastructure**: Local Spark + Airflow in Docker/Podman.
- **ETL logic**:
  - **Extract**: Read CSV events via Spark SQL
  - **Transform**: 
    - Cast columns to correct types (id: long, timestamp: long)
    - Window function: rank by timestamp per (id, name)
    - Keep rank=1 (latest)
    - Aggregate: group by id, create Map<name, value>
  - **Load**: Write partitioned Parquet to `/opt/airflow/data/output/user_settings`

## Setting up a local environment

### Requirements

- Docker or Podman
- Python 3.11 (for local testing)


### Setting up a local environment with Docker
1. Build the image from docker-compose.
```bash
docker-compose build
```
2. Start the container.
```bash
docker-compose up -d
```
3. Launch the Airflow UI at http://localhost:8080. Login with airflow/airflow.

### Setting up a local environment with Podman

1. Build the image from docker-compose.
```bash
podman build -t spark-etl .
```
2. Start the container.
```bash
podman-compose up -d
```
3. Launch the Airflow UI at http://localhost:8080. Login with airflow/airflow.



## Running the DAG

1. Open Airflow UI: http://localhost:8080.
2. Enable the `spark_pipeline` DAG.
3. Trigger manually or wait for daily schedule.
4. Check task logs for Spark output.


## Running unit tests

```bash
docker-compose exec airflow-webserver pytest /opt/airflow/tests/test_spark_job.py -v
```

## Troubleshooting

### Java Path Not Found

In case the java path cannot be found during the execution, run the command inside to container to identify the correct path. Then, update the `JAVA_HOME` variable in the Dockerfile and docker-compose.yml and rebuild the containers:

```
[2025-11-03, 22:30:57 UTC] {subprocess.py:106} INFO - /home/***/.local/lib/python3.11/site-packages/pyspark/bin/spark-class: line 71: /usr/lib/jvm/java-17-openjdk-amd64/bin/java: No such file or directory
```

```bash
docker-compose exec airflow-webserver bash
which java
ls -al /usr/lib/jvm/
```

Stop and rebuild the containers:
```bash
docker-compose down -v
docker-compose up -d --build
```


### Viewing Scheduler Logs

```bash
cat logs/scheduler/2025-10-22/dag_factory_loader.py.log
```
