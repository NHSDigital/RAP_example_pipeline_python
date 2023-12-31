"""
Purpose of the script: configures logging
"""
import sys
import time
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def configure_logging(
    log_folder : str
) -> None:
    """Set up logging format and location to store logs

    Please store logs in a secure location (e.g. IC Green) and not on your local machine as they may contain traces of data.
    
    Parameters
    ----------
        log_folder : str
            directory to store logs
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s -- %(filename)s:\
                %(funcName)5s():%(lineno)s -- %(message)s',
        handlers=[
            logging.FileHandler(str(Path(f".{log_folder}/{time.strftime('%Y-%m-%d_%H-%M-%S')}.log"))),
            logging.StreamHandler(sys.stdout) # Add second handler to print log message to screen
        ]  
    )
    logger = logging.getLogger(__name__)
