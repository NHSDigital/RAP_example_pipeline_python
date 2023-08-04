
from pyspark.sql import functions as F
from pyspark import sql as pyspark

def get_aggregate_counts(
    df_unaggregated : pyspark.DataFrame,
    counting_col : str, 
    alias_name : str = "distinct_count"
) -> pyspark.DataFrame:
    """
        Takes a spark dataframe and column, and returns the distinct count
        of that column

    Parameters
    ----------
        df_unaggregated : pyspark.DataFrame
            The spark dataframe containing the column you want to count
        counting_col : str
            The column you want to get the counts from
        alias_name : 
            The name for the aggregated count column
            defaults to "distinct_count" if no alias_name is passed

    Returns
    -------
        pyspark.DataFrame : 
            A spark datafram with one column (with the alias you specified)
            and one row (the distinct count of values in that column)
    """
    df_aggregated = (df_unaggregated
        .agg(F.countDistinct(counting_col).alias(alias_name))
    )

    return df_aggregated


def get_grouped_aggregate_counts(
    df_unaggregated : pyspark.DataFrame,
    grouping_col : str,
    counting_col : str, 
    alias_name : str = "distinct_count"
) -> pyspark.DataFrame:
    """
        Takes a spark dataframe and column, groups by a specified column, and 
        returns the distinct count of values in another column

    Parameters
    ----------
        df_unaggregated : pyspark.DataFrame
            The spark dataframe containing the column you want to count
        grouping_col : str
            The column you want to group by
        counting_col : str
            The column you want to get the counts from
        alias_name : 
            The name for the aggregated count column
            defaults to "distinct_count" if no alias_name is passed

    Returns
    -------
        pyspark.DataFrame : 
            A spark dataframe with two columns, the column you're grouping by and the distinct count (given
            the alias you specify in alias_name)
            and one row for each group
    """

    df_aggregated = (df_unaggregated
        .groupBy(grouping_col)
        .agg(F.countDistinct(counting_col).alias(alias_name))
        .orderBy(grouping_col)
    )

    return df_aggregated