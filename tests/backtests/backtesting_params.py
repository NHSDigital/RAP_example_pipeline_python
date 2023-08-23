import pathlib

bt_params = {
    'output_base_path': pathlib.Path('./data_out/'),
    'ground_truth_base_path': pathlib.Path('./tests/backtests/ground_truth/'),

    'files_to_compare': [
        {
            'new_output': 'df_hes_england_count/df_hes_england_count.csv',
            'ground_truth': 'hes_england_count_expected_output.csv',
        },
    ]
}
