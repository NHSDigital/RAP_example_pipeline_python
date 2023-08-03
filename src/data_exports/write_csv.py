import os
import glob
from pyspark import sql as pyspark

def save_spark_dataframe_as_csv(
    df_output : pyspark.DataFrame, 
    output_name : str
) -> None:
    '''
    Function to save spark dataframe saved within the outputs dictionary as a csv
    '''

    (df_output
        .coalesce(1)
        .write
        .mode('overwrite')
        .option("header", True)
        .csv(f"data_out/{output_name}/")
    )


def rename_csv_output(
    output_name : str
) -> None:
    '''
    Function to rename default spark file name to the name specified in the outputs dictionary
    '''
    path = rf'data_out/{output_name}/*.csv'
    files = glob.glob(path)
    print(files)
    os.rename(files[0], f'data_out/{output_name}/{output_name}.csv')
