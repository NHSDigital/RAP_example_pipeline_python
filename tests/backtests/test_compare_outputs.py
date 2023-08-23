"""
This script checks whether pairs of CSVs are the same as each other.

To use:
    files_to_compare: [(String, String)] is imported from params.py. It contains pairs of filenames to be tested.
    OUTPUT_DIR: String and GROUND_TRUTH_DIR: String are also imported from params.py. They are the respective locations of the pair of files.

"""

import pandas as pd
import pathlib
from .backtesting_params import bt_params

def test_backtests():

    for backtest in bt_params['files_to_compare']:

        new_output_file = backtest['new_output']
        ground_truth_file = backtest['ground_truth']

        df_output = pd.read_csv(bt_params['output_base_path'] / backtest['new_output'])
        df_ground_truth = pd.read_csv(bt_params['ground_truth_base_path'] / backtest['ground_truth'])

        print(f"\n Testing file: {ground_truth_file} against {new_output_file}")

        pd.testing.assert_frame_equal(df_ground_truth, df_output, check_dtype=True)
