
from pyspark.sql import functions as F

def get_aggregate_counts(df, counting_col, alias_name):
    '''
    Function to get counts in dataframe 
    '''
    df = (df
        .agg(
            F.countDistinct(counting_col).alias(alias_name)
        )
    )

    return df



def get_grouped_aggregate_counts(df, grouping_col, counting_col, alias_name):
    '''
    Function to get counts in dataframe based on grouping
    '''
    df = (df
        .groupBy(grouping_col)
        .agg(
            F.countDistinct(counting_col).alias(alias_name)
        )
        .orderBy(grouping_col)
    )

    return df