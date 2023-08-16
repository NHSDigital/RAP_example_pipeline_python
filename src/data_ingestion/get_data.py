"""Contains functions used to aquire the data from external sources"""

import zipfile
import shutil
import os
import io
from pathlib import Path
import requests


def download_zip_from_url(
    zip_file_url : str, 
    overwrite : bool = False, 
    output_path : str = None
) -> str:
    """Downloads a zipfile from the specified URL
    
    Parameters
    ----------
    zip_file_url : str
        The url string of where the zipfile is held
    overwrite : bool
        if True, then running this again will overwrite existing files of the same name, otherwise 
        it will not.
    output_path : str
        Where you want the zip to be saved to - if left as "None" then it will be saved to 
        "data/{filename}"

    Returns
    ----------
    output_path : str

    """
    filename = Path(zip_file_url).name
    if output_path is None:
        output_path = Path(f"data_in/{filename}")
    else:
        output_path = Path(f"{output_path}/{filename}")
    if output_path.exists():
        if overwrite:
            shutil.rmtree(output_path, ignore_errors=False, onerror=None)
        else:
            raise Exception(f"The zipfile already exists at: {output_path}")

    response = requests.get(zip_file_url, stream=True,timeout=3600)
    downloaded_zip = zipfile.ZipFile(io.BytesIO(response.content))
    downloaded_zip.extractall(output_path)

    return str(output_path)