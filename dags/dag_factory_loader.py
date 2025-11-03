import os
import logging
from pathlib import Path

from dagfactory import load_yaml_dags

# Get logger
logger = logging.getLogger(__name__)

config_dir = "/opt/airflow/dags/dag_configs"

# Log the config file path
logger.info(f"Loading DAGs from config file: {config_dir}")
logger.info(f"Config file exists: {Path(config_dir).exists()}")

load_yaml_dags(
    globals_dict=globals(),
    dags_folder=config_dir,
)
