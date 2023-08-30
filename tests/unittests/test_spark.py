import pytest
import pandas

from src.utils import spark as spark_utils
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def test_create_spark_session():

    test_app_name = 'tests'
    spark = spark_utils.create_spark_session(test_app_name)

    assert spark.__class__.__name__ == 'SparkSession'
    assert spark.sparkContext.appName == test_app_name
    