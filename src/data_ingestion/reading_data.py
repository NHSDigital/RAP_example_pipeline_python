from pyspark import sql as pyspark

def load_csv_into_spark_data_frame(
    spark : pyspark.SparkSession, 
    path_to_csv : str
) -> pyspark.DataFrame:
    """
        loads the data from a CSV at a specified path into a spark dataframe

    Parameters
    ----------
        spark : pyspark.SparkSession
            The SparkSession for the spark app
        path_to_csv : str
            The path to the csv from which you want to create the spark df

    Returns
    -------
        pyspark.DataFrame :
            The spark dataframe holding the data that was in the CSV
    """
    df_from_csv = (spark
        .read
        .csv(path_to_csv, header=True)
    )
    return df_from_csv