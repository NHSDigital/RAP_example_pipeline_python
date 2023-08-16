import os
import glob
from pathlib import Path
from pyspark import sql as pyspark

def save_spark_dataframe_as_csv(
    df_input : pyspark.DataFrame, 
    output_folder : str
) -> None:
    """
    Function to save a spark dataframe as a csv to a new folder in the data_out folder

    Parameters
    ----------
        df_input : pyspark.DataFrame
            The spark dataframe that you want to save as csv
        output_folder : str
            The name for the folder in which the csv file will be saved
    """

    (df_input
        .coalesce(1)
        .write
        .mode('overwrite')
        .option("header", True)
        .csv(str(Path(f"data_out/{output_folder}")))
    )


def rename_csv_output(
    output_name : str
) -> None:
    """
    By default spark gives files saved to csv random filenames.
    This function will check for any CSV files in the specified subdirectory of data_out
    and rename them to the same name as that subdirectory

    Parameters
    ----------
        output_name : str
            The name you want to give to the CSV output. This should be the 
            same name as the folder it is contained in.
    """
    path = rf'data_out/{output_name}/*.csv'
    files = glob.glob(path)
    print(files)
    os.rename(files[0], str(Path(f'data_out/{output_name}/{output_name}.csv')) )
