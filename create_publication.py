"""
Purpose of the script:  to provide a starting point for the basis of your pipeline using example data from SQL server

The script loads Python packages but also internal modules (e.g. modules.helpers, helpers script from the modules folder).
It then loads various configuration variables and a logger, for more info on this find the RAP Community of Practice Repo on Github

Then, we call some basic SQL functions to load in our data, process it and write our outputs to an appropriate file type (e.g. CSV, Excel)
For more info on automated excel outputs, find the automated-excel-publications repo on Gitlab.
"""

# this part imports our Python packages, including our project's modules
import logging
import timeit 
from pathlib import Path
from pyspark.sql import functions as F
import requests

from src.utils.file_paths import get_config
from src.utils.logging_config import configure_logging 
from src.utils.spark import create_spark_session 
from src.data_ingestion.get_data import download_zip_from_url
from src.data_ingestion.reading_data import load_csv_into_spark_data_frame
from src.processing.aggregate_counts import get_aggregate_counts, get_grouped_aggregate_counts
from src.data_exports.write_csv import save_spark_dataframe_as_csv, rename_csv_output


logger = logging.getLogger(__name__)

def main():
    
    # load config, here we load our project's parameters from the config.toml file
    config = get_config() 

    # configure logging
    configure_logging(config['log_dir'], config)
    logger.info(f"Configured logging with log folder: {config['log_dir']}.")

    # get artificial HES data as CSV
    download_zip_from_url(config['data_url'], overwrite=True)
    
    # create spark session
    spark = create_spark_session(config['project_name'])

    # Loading data from CSV as spark data frame
    df_hes_data = load_csv_into_spark_data_frame(spark, config['path_to_downloaded_data'])
    
    # Creating dictionary to hold outputs
    outputs = {}

    # Count number of episodes in England - place this in the outputs dictionary
    outputs["df_hes_england_count"] = get_aggregate_counts(df_hes_data, 'epikey', 'number_of_episodes')

    # Rename and save spark dataframes as CSVs:
    for output_name, output in outputs.items():
        save_spark_dataframe_as_csv(output, output_name)
        rename_csv_output(output_name)


if __name__ == "__main__":
    print(f"Running create_publication script")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    print(f"Running time of create_publication script: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.\n")
