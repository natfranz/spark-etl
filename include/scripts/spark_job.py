from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import LongType


def process_user_events(input_path, output_path):
    """Simplified test: read, filter, write"""
    spark = SparkSession.builder \
        .appName('user_settings_etl') \
        .getOrCreate()

    # Read input
    df = spark.read \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv(input_path)

    # Cast id and timestamp
    df = df.withColumn('id', col('id').cast(LongType())) \
        .withColumn('timestamp', col('timestamp').cast(LongType()))

    print(f"Total events read: {df.count()}")
    df.show()

    # Window rank by timestamp per (id, name)
    window_spec = Window.partitionBy('id', 'name').orderBy(col('timestamp').desc())
    ranked_df = df.withColumn('rn', row_number().over(window_spec))
    latest_df = ranked_df.filter(col('rn') == 1).drop('rn', 'timestamp')

    print(f"Latest events after filtering: {latest_df.count()}")
    latest_df.show()

    # Aggregate to map
    aggregated_df = latest_df.groupBy('id').agg(
        {'value': 'max'}  # Simple aggregation for now
    )

    print(f"Aggregated rows: {aggregated_df.count()}")
    aggregated_df.show()

    # Write output
    aggregated_df.write \
        .mode('overwrite') \
        .parquet(output_path)

    print(f"Output written to {output_path}")

    spark.stop()


def main():
    input_path = '/opt/airflow/data/user_events.csv'
    output_path = '/opt/airflow/data/output/user_settings'
    process_user_events(input_path, output_path)


if __name__ == '__main__':
    main()
