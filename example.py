#!/usr/bin/env python3
"""
run_chunky_csv.py

This script demonstrates how to use the Chunky CSV library to process large CSVs:
  1. Concatenate a folder of CSVs into one master CSV.
  2. Filter the master CSV for uniqueness based on specified columns.
  3. Join the filtered CSV with another large CSV.
  4. Create a pivot table and generate column analytics on the joined data.
  
Make sure that the file 'csv_lib.py' (the Chunky CSV library) is in the same folder (or in your PYTHONPATH).
"""

import os
import csv_lib  # your Chunky CSV library

def main():
    # ===== Step 1: Concatenate a folder of CSVs =====
    input_folder = 'data/csv_folder'  # folder containing your CSV files
    concatenated_csv = os.path.join('data', 'big_input.csv')
    print("=== Concatenating CSVs ===")
    concatenate_csv_folder(input_folder, concatenated_csv)
    print(f"Combined CSV saved as: {concatenated_csv}")
    
    # ===== Step 2: Filter for Unique Rows =====
    # Define the columns that together define uniqueness.
    unique_cols = ['id', 'name']  # change to your unique key columns
    unique_output = os.path.join('data', 'unique_filtered.csv')
    print("=== Filtering for Unique Rows ===")
    csv_lib.unique_filter(concatenated_csv, unique_cols, unique_output, chunksize=10000)
    print(f"Unique-filtered CSV saved as: {unique_output}")
    
    # ===== Step 3: Join the CSV with Another Large CSV =====
    join_file = os.path.join('data', 'other_large.csv')  # the CSV to join with
    joined_output = os.path.join('data', 'joined_output.csv')
    join_key = 'id'  # the common column used for joining (adjust as needed)
    print("=== Joining CSV Files ===")
    csv_lib.join_large_csvs(unique_output, join_file, left_on=join_key, right_on=join_key,
                             join_type='left', chunksize=50000, output_csv=joined_output)
    print(f"Joined CSV saved as: {joined_output}")
    
    # ===== Step 4a: Create a Pivot Table =====
    # For example, pivot by 'category' to count rows per 'id' and 'category'
    pivot_output = os.path.join('data', 'pivot_output.csv')
    index_cols = ['id']          # rows identified by 'id'
    pivot_cols = ['category']    # pivot (columns) defined by 'category'
    value_col = 'amount'         # the value column (for aggregation; can be any numeric column)
    print("=== Creating Pivot Table ===")
    csv_lib.streaming_pivot_table(joined_output,
                                  index_cols=index_cols,
                                  pivot_cols=pivot_cols,
                                  value_col=value_col,
                                  aggfunc='count',
                                  chunksize=10000,
                                  output_csv=pivot_output)
    print(f"Pivot table CSV saved as: {pivot_output}")
    
    # ===== Step 4b: Generate Column Analytics =====
    analytics_output = os.path.join('data', 'column_analytics.csv')
    print("=== Generating Column Analytics ===")
    csv_lib.generate_column_analytics(joined_output, output_csv=analytics_output, chunksize=10000)
    print(f"Column analytics CSV saved as: {analytics_output}")

if __name__ == '__main__':
    main()
