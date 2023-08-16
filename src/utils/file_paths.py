"""
Purpose of the script: loads config
"""
import logging
import toml
import pathlib

logger = logging.getLogger(__name__)

def get_config(
    toml_path : str="config.toml"
) -> dict:
    """Gets the config toml from the root directory and returns it as a dict. Can be called from any file in the project

    Parameters
    ----------
        toml_path : str
            Path, filename, and extension of the toml config file.
            Defaults to config.toml

    Returns
    -------
        Dict : 
            A dictionary containing details of the database, paths, etc. Should contain all the things that will 
            change from one run to the next

    Example
    -------
        from shmi_improvement.utilities.helpers import get_config
        config = get_config()
    """
    return toml.load(pathlib.Path(toml_path))
    