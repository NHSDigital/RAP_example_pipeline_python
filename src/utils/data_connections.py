"""
Purpose of script: handles reading data in and writing data back out.
"""
import logging
import pathlib
from textwrap import dedent
import duckdb
import pandas as pd

DB_PATH = pathlib.Path(".db")  # Default database path
logger = logging.getLogger(__name__)


def get_duckdb_connection(db_path: pathlib.Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """
    Create a connection to the duckdb database at the given path.

    Parameters
    ----------
    db_path : pathlib.Path, optional
        Path to the database file, by default DB_PATH

    Returns
    -------
    duckdb.DuckDBPyConnection
        Connection object.
    """
    conn = duckdb.connect(str(db_path.resolve()))
    conn.execute("SET GLOBAL pandas_analyze_sample=100000")
    return conn


def create_table(
    conn: duckdb.DuckDBPyConnection,
    source: str,
    table_name: str,
    replace: bool = False,
    exists_ok: bool = True,
) -> str:
    """
    Create DuckDB table.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection.
    source : str
        Specifies the source in duckdb syntax. Can be a query string
        or a file source (e.g. https://duckdb.org/docs/guides/import/csv_import).
    table_name: str
        Name of the table to be created.
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
    if replace:
        create_expr = "CREATE OR REPLACE TABLE"
    elif exists_ok:
        create_expr = "CREATE TABLE IF NOT EXISTS"
    else:
        create_expr = "CREATE TABLE"

    query = f"""
        {create_expr} {table_name}
        AS
        SELECT * FROM {source}
    """
    conn.sql(dedent(query))

    return table_name


def create_table_from_csv(
    conn: duckdb.DuckDBPyConnection,
    csv_path: pathlib.Path,
    replace: bool = False,
    exists_ok: bool = True,
) -> str:
    """
    Create DuckDB table using a csv source.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection.
    csv_path : pathlib.Path
        Path to the csv file.
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
    return create_table(
        conn,
        source=f"read_csv_auto('{csv_path.resolve()}')",
        table_name=csv_path.stem,
        replace=replace,
        exists_ok=exists_ok,
    )


def read_table_to_df(conn: duckdb.DuckDBPyConnection, table_name: str) -> pd.DataFrame:
    """
    Read data from a duckdb table into a pandas dataframe.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection.
    table_name : str
        Name of table to query.

    Returns
    -------
    pd.DataFrame
        DataFrame from the query.
    """
    query = f"SELECT * FROM {table_name}"
    df = conn.sql(query).df()
    return df


def write_df_to_table(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table_name: str,
    replace: bool = False,
    exists_ok: bool = True,
) -> str:
    """
    Create DuckDB table using a pandas dataframe.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Database connection.
    df : pd.DataFrame
        Pandas dataframe to create the table with.
    table_name : str
        Name of the table to create.
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
    # NOTE: duckdb can read 'df' based on referencing via a string, so it
    # looks like df is unused from the arguments, but that is just the syntax
    # highlighting not realsing!
    return create_table(
        conn, source="df", table_name=table_name, replace=replace, exists_ok=exists_ok
    )
