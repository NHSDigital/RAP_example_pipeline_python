from pyspark.sql import SparkSession

def create_spark_session(app_name):
    spark_session = (SparkSession
        .builder
        .appName(app_name)
        .getOrCreate()
    )

    return spark_session