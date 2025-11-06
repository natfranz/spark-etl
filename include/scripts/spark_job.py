from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, map_from_arrays, collect_list
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
import logging

logging.basicConfig(level=logging.INFO)


def create_spark_session(app_name: str) -> SparkSession:
    """Initialize and return Spark session."""
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logging.info(f"Spark session created: {app_name}")
        return spark
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        raise


def extract_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the CSV file and return DataFrame."""
    try:
        logging.info(f"Reading data from {input_path}")
        df = spark.read.option('header', 'true').csv(input_path)

        if df.count() == 0:
            logging.warning(f"No data found in {input_path}")

        return df
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            logging.error(f"File not found: {input_path}")
            raise FileNotFoundError(f"Input file does not exist: {input_path}")
        else:
            logging.error(f"Analysis error while reading file: {e}")
            raise
    except Exception as e:
        logging.error(f"Unexpected error reading file: {e}")
        raise


def cast_columns(df: DataFrame) -> DataFrame:
    """Cast columns to correct data types."""
    logging.info("Casting columns to correct types")
    return df.withColumn('id', col('id').cast('long')) \
        .withColumn('timestamp', col('timestamp').cast('long'))


def get_latest_records(df: DataFrame) -> DataFrame:
    """Keep only the row with max timestamp for each (id, name)."""
    logging.info("Filtering to latest records per id and name")
    window = Window.partitionBy('id', 'name').orderBy(col('timestamp').desc())

    return df.withColumn('rank', row_number().over(window)) \
        .filter(col('rank') == 1) \
        .select('id', 'name', 'value')


def create_settings_map(df: DataFrame) -> DataFrame:
    """Group by id and create map from name and value."""
    logging.info("Creating settings map")
    return df.groupBy('id').agg(
        map_from_arrays(
            collect_list('name'),
            collect_list('value')
        ).alias('settings')
    )


def transform_data(df: DataFrame) -> DataFrame:
    """Combine all transformation steps."""
    logging.info("Starting data transformation pipeline")

    df = cast_columns(df)
    df = get_latest_records(df)
    df = create_settings_map(df)

    return df


def load_data(df: DataFrame, output_path: str) -> None:
    """Write DataFrame to parquet."""
    try:
        logging.info(f"Writing data to {output_path}")
        df.write.mode('overwrite').parquet(output_path)
        logging.info(f"Successfully wrote data to {output_path}")
    except AnalysisException as e:
        if "Cannot overwrite" in str(e):
            logging.error(f"Cannot overwrite path: {output_path}")
            raise
        else:
            logging.error(f"Analysis error while writing file: {e}")
            raise
    except Exception as e:
        logging.error(f"Unexpected error writing file: {e}")
        raise


def main():
    spark = None

    try:
        app_name = 'user_settings_etl'
        input_path = '/opt/airflow/data/user_events.csv'
        output_path = '/opt/airflow/data/output/user_settings'

        spark = create_spark_session(app_name)

        df = extract_data(spark, input_path)
        df_transformed = transform_data(df)

        df_transformed.show(truncate=False)

        load_data(df_transformed, output_path)

        logging.info("ETL pipeline completed successfully")

    except FileNotFoundError as e:
        logging.error(f"File error: {e}")

    except AnalysisException as e:
        logging.error(f"Spark analysis error: {e}")

    except Exception as e:
        logging.error(f"Pipeline failed with error: {e}", exc_info=True)
        raise
    finally:
        if spark:
            logging.info("Stopping Spark session")
            spark.stop()


if __name__ == '__main__':
    main()
