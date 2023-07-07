
from pyspark.sql import functions as F

def get_aggregate_counts(df, grouping_col, counting_col, alias_name):
    '''
    Function to get counts in dataframe based on grouping
    Note: Setting grouping_col as None will aggregate all data
    '''
    if grouping_col:
        df = (df
            .groupBy(grouping_col)
        )

    df = (df
        .agg(
            F.countDistinct(counting_col).alias(alias_name)
        )
    )
    return df