from pyspark import sql as pyspark

def create_spark_session(
    app_name = "spark_pipeline" : str
) -> pyspark.SparkSession:
    """
        Creates a spark session: this is needed to run PySpark code.

    Parameters
    ----------
        app_name : str
            the name of the Spark application
            Defaults to "spark_pipeline"

    Returns
    -------
        pyspark.SparkSession
            the SparkSession object
    """
    spark_session = (pyspark.SparkSession
        .builder
        .appName(app_name)
        .getOrCreate()
    )

    return spark_session