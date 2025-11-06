import logging
from pathlib import Path

from dagfactory import load_yaml_dags

config_dir = "/opt/airflow/dags/dag_configs"

logging.info(f"Loading DAGs from config file: {config_dir}")
logging.info(f"Config file exists: {Path(config_dir).exists()}")

load_yaml_dags(
    globals_dict=globals(),
    dags_folder=config_dir,
)
