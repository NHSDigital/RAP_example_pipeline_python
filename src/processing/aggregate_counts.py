
from pyspark.sql import functions as F
from pyspark import sql as pyspark

def get_aggregate_counts(
    df_unaggregated : pyspark.DataFrame,
    counting_col : str, 
    alias_name: str
) -> pyspark.DataFrame:
    '''
    Function to get counts in dataframe 
    '''
    df_aggregated = (df_unaggregated
        .agg(F.countDistinct(counting_col).alias(alias_name))
    )

    return df_aggregated


def get_grouped_aggregate_counts(
    df_unaggregated : pyspark.DataFrame,
    grouping_col : str,
    counting_col : str, 
    alias_name: str
) -> pyspark.DataFrame:
    '''
    Function to get counts in dataframe based on grouping
    '''
    df_aggregated = (df_unaggregated
        .groupBy(grouping_col)
        .agg(F.countDistinct(counting_col).alias(alias_name))
        .orderBy(grouping_col)
    )

    return df_aggregated