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

        try:
            # confirm dfs are equal       
            assert df_output.equals(df_ground_truth)
            print(f"\nTest status: PASS \nTest Details: CSV ground truth: {ground_truth_file} and CSV output: {new_output_file} are equal.\n")
        except AssertionError:
            print(f"Test status: FAILED \nTest Details: {new_output_file} is not equal to {ground_truth_file}.\n")

            # If we get to here, the backtest has failed, but the follow up tests below may help us narrow down
            # where the difference is coming from:
            try:
                # test csvs have the same number of rows and cols       
                assert df_output.shape == df_ground_truth.shape
                print(f"\nTest status: PASS \nTest Details: CSV ground truth: {ground_truth_file} and CSV output: {new_output_file} have the same number of rows and cols.\n")
            except:
                print(f"Test status: FAILED \nTest Details: Number of rows, cols {df_output.shape} in {new_output_file} is different to {df_ground_truth.shape} in {ground_truth_file}.\n")

            try:
                #test the columns have the same names and same order
                assert df_output.columns.tolist() == df_ground_truth.columns.tolist()
                print(f"Test status: PASS \nTest Details: CSV ground truth: {ground_truth_file} and CSV output: {new_output_file} have the same column names and order.\n")
            except:
                print(f"Test status: FAILED \nTest Details: {new_output_file} has different column names to {ground_truth_file}. {df_output.columns.tolist()} compared to {df_ground_truth.columns.tolist()}.\n")

            # test if the contents of each column are the same
            for col in df_ground_truth.columns:
                try:
                    assert (df_ground_truth[col].equals(df_output[col]))
                    print(f"Test status: PASS \nTest Details: Column {col} in CSV ground truth {ground_truth_file} and CSV output {new_output_file} have the same column contents.\n")
                except:
                    print(f'Test status: FAILED \nTest Details: Column comparison: The contents in column {col} are different between {new_output_file} and {ground_truth_file}.') 

            # assert False
