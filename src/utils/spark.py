from pyspark.sql import SparkSession

def create_spark_session(app_name):
    """Creates a spark session: this is needed to run PySpark code."""
    spark_session = (SparkSession
        .builder
        .appName(app_name)
        .getOrCreate()
    )

    return spark_session