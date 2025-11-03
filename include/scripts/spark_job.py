from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('csv_processor').getOrCreate()

# Read CSV
df = spark.read.csv('/opt/airflow/data/sample.csv', header=True, inferSchema=True)

# Transform
result = df.filter(df.value > 10).select('id', 'value')

# Write output
result.coalesce(1).write.mode('overwrite').csv('/opt/airflow/data/output/spark_result')

spark.stop()
