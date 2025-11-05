# airflow-server



## Project structure
```

```
## Setting up a local environment

### Setting up a local environment with Docker
```bash
docker-compose build
```
```bash
docker-compose up -d
```

### Setting up a local environment with Podman

1. Build the image from docket-compose.
```bash
podman build -t spark-etl .
```
2. Start the container.
```bash
podman-compose up -d
```

Rebuild:
docker-compose up -d --build

4. Launch the Airflow UI at http://localhost:8080 (login with airflow/airflow).


### Other useful commands

Stop the container:
```bash
podman-compose down
```

Read the scheduler logs:

```bash
cat logs/scheduler/2025-10-22/dag_factory_loader.py.log
```
Running Your Tests Inside the Container

docker-compose exec airflow-webserver pytest /opt/airflow/tests/ -v
docker-compose exec airflow-webserver pytest /opt/airflow/include/tests/ -v


docker-compose exec airflow-webserver bash
which java
ls -al /usr/lib/jvm/

docker-compose exec airflow-webserver pytest /opt/airflow/tests/test_spark_job.py -v
