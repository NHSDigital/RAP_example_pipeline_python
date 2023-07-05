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
from src.utils.file_paths import get_config
from src.utils.logging_config import configure_logging 


from pyspark.sql import SparkSession
import requests
from src.data_ingestion.get_data import download_zip_from_url


logger = logging.getLogger(__name__)

def main():
    
    # load config, here we load our project's parameters from the config.toml file
    config = get_config("config.toml") 
    filled_value = config['filled_value']
    output_dir = Path(config['output_dir'])
    log_dir = Path(config['log_dir'])

    # configure logging
    configure_logging(log_dir, config)
    logger.info(f"Configured logging with log folder: {log_dir}.")

 
    # get artificial HES data as CSV
    ARTIFICIAL_HES_URL = "https://s3.eu-west-2.amazonaws.com/files.digital.nhs.uk/assets/Services/Artificial+data/Artificial+HES+final/artificial_hes_ae_202302_v1_sample.zip"
    download_zip_from_url(ARTIFICIAL_HES_URL,overwrite=True)
 
    
    # create spark session
    spark = (SparkSession
        .builder
        .appName('example_pipeline_pyspark_version')
        .getOrCreate()
    )

    # load latest CSV into dataframe
    df_hes_data = (spark
        .read
        .csv("data/artificial_hes_ae_202302_v1_sample.zip/artificial_hes_ae_202302_v1_sample/artificial_hes_ae_2122.csv", header=True)
    )


    # follow data processing steps



    # produce outputs
    for table_name, df in publication_breakdowns.items():
        df.to_csv(output_dir / f'{table_name}.csv', index=False)
        logger.info('\n\n%s.csv created!\n', table_name)
    logger.info(f"Produced output(s) in folder: {output_dir}.")
    
if __name__ == "__main__":
    print(f"Running create_publication script")
    start_time = timeit.default_timer()
    main()
    total_time = timeit.default_timer() - start_time
    print(f"Running time of create_publication script: {int(total_time / 60)} minutes and {round(total_time%60)} seconds.\n")
