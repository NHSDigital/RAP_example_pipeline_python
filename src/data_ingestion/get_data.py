"""Contains functions used to aquire the data from external sources"""

from typing import Any, Literal
import zipfile
import shutil
import os
import io
import pathlib
import requests
import pathlib
from textwrap import dedent
import duckdb

DB_PATH = pathlib.Path(".db")
DATA_PATH = pathlib.Path(__file__).parent.parent.parent / "data"
INPUT_PATH = DATA_PATH / "input"
ARTIFICIAL_HES_BASE_URL = f"https://s3.eu-west-2.amazonaws.com/files.digital.nhs.uk/assets/Services/Artificial+data/Artificial+HES+final"


def download_zip_from_url(
    zip_file_url: str, overwrite: bool = False, output_path: pathlib.Path = None
) -> str:
    """Downloads a zipfile from the specified URL

    Parameters
    ----------
    zip_file_url : str
        The url string of where the zipfile is held
    overwrite : bool
        if True, then running this again will overwrite existing files of the same name, otherwise
        it will not.
    output_path : pathlib.Path
        Where you want the zip to be saved to - if left as "None" then it will be saved to
        "data/{filename}"

    Returns
    ----------
    output_path : pathlib.Path

    """
    filename = pathlib.Path(zip_file_url).stem

    if output_path is None:
        output_path = INPUT_PATH / filename
    if os.path.exists(output_path) and overwrite is True:
        shutil.rmtree(output_path, ignore_errors=False, onerror=None)
    elif os.path.exists(output_path) and overwrite is not True:
        raise Exception(f"The zipfile already exists at: {output_path}")

    response = requests.get(zip_file_url, stream=True, timeout=3600)
    downloaded_zip = zipfile.ZipFile(io.BytesIO(response.content))
    downloaded_zip.extractall(output_path)
    return output_path


def create_table_from_csv(
    csv_path: pathlib.Path,
    conn: duckdb.DuckDBPyConnection,
    replace: bool = False,
    exists_ok: bool = True,
) -> str:
    """
    Create DuckDB table using a csv source.

    Parameters
    ----------
    csv_path : pathlib.Path
        Path to the csv file.
    conn : duckdb.DuckDBPyConnection
        Database connection.
    replace : bool, optional
        Should the table be replaced if it exists, by default False
    exists_ok : bool, optional
        Raise errors if the table already exists, by default True.
        If replace=True then this parameter has no effect.

    Returns
    -------
    str
        Name of the table that was created.
    """
    table_name = csv_path.stem

    if replace:
        create_expr = "CREATE OR REPLACE TABLE"
    elif exists_ok:
        create_expr = "CREATE TABLE IF NOT EXISTS"
    else:
        create_expr = "CREATE TABLE"

    stmt = f"""
        {create_expr} {table_name}
        AS
        SELECT * FROM read_csv_auto('{csv_path.resolve()}')
    """
    conn.execute(dedent(stmt))

    return table_name


def download_artificial_hes_zip(
    dataset_name: Literal["ae", "apc", "op"],
    version: str = "202302_v1",
    size: Literal["sample", "full"] = "sample",
) -> pathlib.Path:
    """
    Download and unpack artificial hes zip file.

    Parameters
    ----------
    dataset_name : Literal["ae", "apc", "op"]
        Name of dataset to download.
    version : str, optional
        Version to download, by default "202302_v1"
    size : str, optional
        Size to download, by default "sample"

    Returns
    -------
    pathlib.Path
        Path to the downloaded file.
    """
    zip_name = f"artificial_hes_{dataset_name}_{version}_{size}.zip"
    zip_url = f"{ARTIFICIAL_HES_BASE_URL}/{zip_name}"
    zip_path = download_zip_from_url(zip_url, overwrite=True)
    return zip_path


def get_user_inputs() -> dict[str, Any]:
    """
    Get user inputs to configure the main function.

    Returns
    -------
    dict[str, Any]
        User input variables.
    """
    replace = input("Replace tables if they already exist? (y/n, default=n): ")
    replace = replace == "y"

    if not replace:
        exists_ok = input(
            "Continue without error if tables already exist? (y/n, default=y): "
        )
        exists_ok = exists_ok == "" or exists_ok == "y"
    else:
        exists_ok = True

    return {
        "replace": replace,
        "exists_ok": exists_ok,
    }


if __name__ == "__main__":
    user_inputs = get_user_inputs()

    conn = duckdb.connect(str(DB_PATH.resolve()))
    zip_path = download_artificial_hes_zip("ae")

    for csv_path in INPUT_PATH.glob("**/*.csv"):
        table_name = create_table_from_csv(
            csv_path,
            conn,
            replace=user_inputs["replace"],
            exists_ok=user_inputs["exists_ok"],
        )

    df = conn.execute(f"SELECT * FROM {table_name} LIMIT 10").fetch_df()

    print(f"Printing results from table '{table_name}'")
    print(df)
