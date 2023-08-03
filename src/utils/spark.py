from pyspark import sql as pyspark

def create_spark_session(
    app_name : str
) -> pyspark.SparkSession:
    """Creates a spark session: this is needed to run PySpark code."""
    spark_session = (pyspark.SparkSession
        .builder
        .appName(app_name)
        .getOrCreate()
    )

    return spark_session