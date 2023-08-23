
from pyspark.sql import functions as F

def get_distinct_count(df_unaggregated, counting_col, alias_name):
    '''
    Function to get counts in dataframe 
    '''
    df_aggregated = (df_unaggregated
        .agg(F.countDistinct(counting_col).alias(alias_name))
    )

    return df_aggregated


def get_grouped_distinct_counts(df_unaggregated, grouping_col, counting_col, alias_name):
    '''
    Function to get counts in dataframe based on grouping
    '''
    df_aggregated = (df_unaggregated
        .groupBy(grouping_col)
        .agg(F.countDistinct(counting_col).alias(alias_name))
        .orderBy(grouping_col)
    )

    return df_aggregated