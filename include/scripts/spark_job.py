from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, map_from_arrays, collect_list
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder.appName('user_settings_etl').getOrCreate()

    df = spark.read.option('header', 'true').csv('/opt/airflow/data/user_events.csv')

    # Cast columns to correct types
    df = df.withColumn('id', col('id').cast('long')) \
        .withColumn('timestamp', col('timestamp').cast('long'))

    # Keep only the row with max timestamp for each (id, name)
    window = Window.partitionBy('id', 'name').orderBy(col('timestamp').desc())
    df_latest = df.withColumn('rank', row_number().over(window)) \
        .filter(col('rank') == 1) \
        .select('id', 'name', 'value')

    # Group by id and create map from name->value
    df_result = df_latest.groupBy('id').agg(
        map_from_arrays(
            collect_list('name'),
            collect_list('value')
        ).alias('settings')
    )

    # Print the transformed results
    df_result.show(truncate=False)

    df_result.write.mode('overwrite').parquet('/opt/airflow/data/output/user_settings')

    spark.stop()


if __name__ == '__main__':
    main()
