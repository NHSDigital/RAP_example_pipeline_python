from pyspark import sql as pyspark
from pathlib import Path

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
            The path to the csv you want to load into a spark df as a string

    Returns
    -------
        pyspark.DataFrame :
            A spark dataframe containing the data that was in the CSV
    """
    df_from_csv = (spark
        .read
        .csv(path_to_csv, header=True)
    )
    return df_from_csv
