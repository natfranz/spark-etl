import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from include.scripts.spark_job import process_user_events  # Import for main, but test modularly
from pyspark.sql.types import StructType, StructField, StringType, LongType


@pytest.fixture(scope='session')
def spark():
    spark_session = SparkSession.builder \
        .master('local[1]') \
        .appName('test_settings_etl') \
        .getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def example_data(spark):
    """Create DataFrame from assignment example"""
    schema = StructType([
        StructField('id', LongType(), True),
        StructField('name', StringType(), True),
        StructField('value', StringType(), True),
        StructField('timestamp', LongType(), True)
    ])
    data = [
        (1, 'notification', 'true', 1546333200),
        (3, 'refresh', 'denied', 1546334200),
        (2, 'background', 'notDetermined', 1546333611),
        (3, 'refresh', '4', 1546333443),
        (1, 'notification', 'false', 1546335647),
        (1, 'background', 'true', 1546333546),
    ]
    return spark.createDataFrame(data, schema)


def test_transformation_selects_latest_timestamp_per_name(spark, example_data):
    """Test core logic: picks value with max timestamp per (id, name)"""
    # Simulate process_user_events without I/O: apply window rank and filter
    window_spec = Window.partitionBy('id', 'name').orderBy(col('timestamp').desc())
    from pyspark.sql import Window
    from pyspark.sql.functions import row_number
    ranked_df = example_data.withColumn('rn', row_number().over(window_spec))
    latest_df = ranked_df.filter(col('rn') == 1).drop('rn', 'timestamp')

    # Expected latest values
    expected_count = 4  # 1-notification(false), 1-background(true), 2-background, 3-refresh(denied)
    assert latest_df.count() == expected_count

    # Verify specific latest selections
    latest_for_id1 = latest_df.filter(col('id') == 1).collect()
    assert len(latest_for_id1) == 2
    assert any(row['name'] == 'notification' and row['value'] == 'false' for row in latest_for_id1)
    assert any(row['name'] == 'background' and row['value'] == 'true' for row in latest_for_id1)

    latest_for_id3 = latest_df.filter(col('id') == 3).collect()
    assert len(latest_for_id3) == 1
    assert latest_for_id3[0]['value'] == 'denied'  # Higher timestamp 1546334200 > 1546333443


def test_aggregation_creates_correct_map(spark, example_data):
    """Test map aggregation: name → latest value per id"""
    # Simulate full aggregation
    window_spec = Window.partitionBy('id', 'name').orderBy(col('timestamp').desc())
    from pyspark.sql import Window
    from pyspark.sql.functions import row_number, collect_list, struct, create_map
    ranked_df = example_data.withColumn('rn', row_number().over(window_spec))
    latest_df = ranked_df.filter(col('rn') == 1).drop('rn', 'timestamp')

    # Aggregate to map
    from pyspark.sql.functions import size
    aggregated_df = latest_df.groupBy('id').agg(
        create_map(
            collect_list(struct('name', 'value'))  # Simplified map creation
        ).alias('settings')
    )

    # Expected results
    results = aggregated_df.collect()
    assert len(results) == 3  # One per unique id

    # Verify id=1 map
    id1_row = [r for r in results if r['id'] == 1][0]
    settings1 = id1_row['settings']
    assert settings1['notification'] == 'false'
    assert settings1['background'] == 'true'

    # Verify id=3: refresh → denied (latest)
    id3_row = [r for r in results if r['id'] == 3][0]
    assert id3_row['settings']['refresh'] == 'denied'


def test_end_to_end_with_temp_files(spark, example_data, tmp_path):
    """Test full process_user_events (I/O included) with example data"""
    input_path = str(tmp_path / 'input')
    output_path = str(tmp_path / 'output')

    # Write example data as CSV
    example_data.write.mode('overwrite').option('header', 'true').csv(input_path)

    # Run full function
    output_count = process_user_events(input_path, output_path)

    # Read output and verify
    result_df = spark.read.parquet(output_path)
    assert result_df.count() == 3  # 3 unique ids
    assert output_count == 3

    # Verify map structure and values match example
    id1_result = result_df.filter(col('id') == 1).collect()[0]
    assert id1_result['settings']['notification'] == 'false'
    assert id1_result['settings']['background'] == 'true'
    assert id1_result['settings']['refresh'] is None  # Not present for id=1


def test_empty_input_produces_empty_output(spark, tmp_path):
    """Edge case: No events → empty partitioned table"""
    empty_df = spark.createDataFrame([], schema='id long, name string, value string, timestamp long')
    input_path = str(tmp_path / 'input')
    output_path = str(tmp_path / 'output')

    empty_df.write.mode('overwrite').option('header', 'true').csv(input_path)

    output_count = process_user_events(input_path, output_path)
    result_df = spark.read.parquet(output_path)

    assert output_count == 0
    assert result_df.count() == 0
