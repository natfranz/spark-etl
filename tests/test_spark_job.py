import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from include.scripts.spark_job import transform_data, cast_columns, get_latest_records, create_settings_map


@pytest.fixture(scope='session')
def spark():
    spark_session = SparkSession.builder.master('local[1]').appName('test').getOrCreate()
    yield spark_session
    spark_session.stop()


def test_transform_data_end_to_end(spark):
    """Test full transformation pipeline with example data"""
    schema = StructType([
        StructField('id', StringType()),
        StructField('name', StringType()),
        StructField('value', StringType()),
        StructField('timestamp', StringType())
    ])
    data = [
        ('1', 'notification', 'true', '1546333200'),
        ('1', 'notification', 'false', '1546335647'),
        ('1', 'background', 'true', '1546333546'),
        ('2', 'background', 'notDetermined', '1546333611'),
        ('3', 'refresh', '4', '1546333443'),
        ('3', 'refresh', 'denied', '1546334200')
    ]
    df = spark.createDataFrame(data, schema)
    result = transform_data(df)
    rows = {int(row['id']): row['settings'] for row in result.collect()}

    assert rows[1]['notification'] == 'false'
    assert rows[1]['background'] == 'true'
    assert rows[2]['background'] == 'notDetermined'
    assert rows[3]['refresh'] == 'denied'


def test_get_latest_records(spark):
    """Test that only latest timestamp per (id, name) is kept"""
    schema = StructType([
        StructField('id', LongType()),
        StructField('name', StringType()),
        StructField('value', StringType()),
        StructField('timestamp', LongType())
    ])
    data = [
        (1, 'notification', 'true', 1546333200),
        (1, 'notification', 'false', 1546335647)
    ]
    df = spark.createDataFrame(data, schema)
    df = df.withColumn('id', df.id.cast('long')).withColumn('timestamp', df.timestamp.cast('long'))

    result = get_latest_records(df)
    assert result.count() == 1
    assert result.collect()[0]['value'] == 'false'


def test_create_settings_map(spark):
    """Test map creation from name and value"""
    schema = StructType([
        StructField('id', LongType()),
        StructField('name', StringType()),
        StructField('value', StringType())
    ])
    data = [
        (1, 'notification', 'false'),
        (1, 'background', 'true')
    ]
    df = spark.createDataFrame(data, schema)

    result = create_settings_map(df)
    row = result.collect()[0]
    assert row['settings']['notification'] == 'false'
    assert row['settings']['background'] == 'true'
