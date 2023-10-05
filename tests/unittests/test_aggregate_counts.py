import pytest
import pandas

from src.processing import aggregate_counts as aggregate_counts
from src.utils import spark as spark_utils
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def test_distinct_count():
    """
    Tests get_distinct_counts
    """
    spark = spark_utils.create_spark_session('tests')

    expected_data = [
        (3,),
    ]
    expected_cols = ['count']
    df_expected = spark.createDataFrame(expected_data, expected_cols)

    unaggregated_data = [
        ('group_1',),
        ('group_2',),
        ('group_2',),
        ('group_3',),
        ('group_3',),
        ('group_3',),
    ]
    unaggregated_cols = ['group_name']
    df_unaggregated = spark.createDataFrame(unaggregated_data, unaggregated_cols)

    df_actual = aggregate_counts.get_distinct_count(df_unaggregated, 'group_name', 'count')

    assert df_actual.toPandas().equals(df_expected.toPandas())
    

def test_get_grouped_distinct_counts():
    """
    Tests get_distinct_counts
    """
    spark = spark_utils.create_spark_session('tests')

    expected_data = [
        ('group_1', 1),
        ('group_2', 1),
        ('group_3', 2),
    ]
    expected_cols = ['group_name', 'count']
    df_expected = spark.createDataFrame(expected_data, expected_cols)

    unaggregated_data = [
        ('group_1', '1'),
        ('group_2', '1'),
        ('group_2', '1'),
        ('group_3', '1'),
        ('group_3', '2'),
    ]
    unaggregated_cols = ['group_name', 'values']
    df_unaggregated = spark.createDataFrame(unaggregated_data, unaggregated_cols)

    df_actual = aggregate_counts.get_grouped_distinct_counts(df_unaggregated, 'group_name', 'values', 'count')

    assert df_actual.toPandas().equals(df_expected.toPandas())
