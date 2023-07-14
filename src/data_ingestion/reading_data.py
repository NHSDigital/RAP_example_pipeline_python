def load_csv_into_spark_data_frame(spark, data_hes_path):
    '''
    A function to load the data from CSV into a spark dataframe using path defined
    '''
    df_hes_data = (spark
        .read
        .csv(data_hes_path, header=True)
    )
    return df_hes_data