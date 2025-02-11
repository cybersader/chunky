#!/usr/bin/env python3
"""
csv_lib.py – A minimal CSV transformation library using streaming/chunked processing.

Functions include:
  • streaming_pivot_table() – Build a pivot table (aggregation) without loading the entire CSV.
  • process_csv_remove_parentheses() – Remove text in parentheses from selected columns.
  • generate_column_analytics() – Stream through a CSV to compute per‑column stats.
  • join_large_csvs() – Join two CSV files by reading the “left” file in chunks.
  • unique_filter() – Write only unique rows (based on specified columns) to an output CSV.
  • concatenate_csv_folder() – Concatenate all CSV files in a folder into one CSV.

Each function writes its output as a CSV file and displays progress bars.
"""

import os
import re
import csv
import tempfile
import shutil
import glob
import pandas as pd
from tqdm import tqdm
import humanize  # Optional: used to “humanize” rates in progress bars

# -------------------------
# Custom progress bar classes
# -------------------------

class CustomCSVTqdm(tqdm):
    @staticmethod
    def format_meter(n, total, elapsed, rate_fmt=None, postfix=None, ncols=None, **extra_kwargs):
        rate = n / elapsed if elapsed else 0
        remaining_time = (total - n) / rate if rate and total is not None else 0
        formatted_rate = f"{rate:.2f}"
        humanized_rate = humanize.intcomma(formatted_rate) if humanize else formatted_rate
        if total is not None:
            return (f"Counting rows: {n}/{total} rows "
                    f"[{tqdm.format_interval(elapsed)}<{tqdm.format_interval(remaining_time)}, "
                    f"{humanized_rate} row/s]")
        else:
            return (f"Counting rows: {n} rows "
                    f"[{tqdm.format_interval(elapsed)}, {humanized_rate} row/s]")

# -------------------------
# Helper functions for chunking
# -------------------------

def count_rows(file_path, chunksize=10000):
    """Count the total number of rows in a CSV by reading it in chunks."""
    total = 0
    for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
        total += chunk.shape[0]
    return total

def read_csv_in_chunks(file_path, chunksize=10000):
    """Generator yielding DataFrame chunks from a CSV file."""
    reader = pd.read_csv(file_path, chunksize=chunksize, low_memory=False)
    for chunk in reader:
        yield chunk

# -------------------------
# 1. Streaming pivot table
# -------------------------

def streaming_pivot_table(input_csv, index_cols, pivot_cols, value_col, aggfunc='count', chunksize=10000, output_csv=None):
    """
    Build a pivot table from a large CSV without loading the entire file.
    
    Parameters:
      input_csv   : Path to the input CSV.
      index_cols  : List of column names to use as the pivot table’s index.
      pivot_cols  : List of column names to pivot on.
      value_col   : The column whose values will be aggregated.
      aggfunc     : Aggregation function: supports 'count', 'sum', or 'nunique'.
      chunksize   : Number of rows to process per chunk.
      output_csv  : (Optional) Path for the output CSV. If None, a filename is generated.
    
    Returns:
      The output CSV filename.
    """
    agg_dict = {}  # key: (index_tuple, pivot_tuple) → aggregated value (or set for 'nunique')
    
    for chunk in tqdm(read_csv_in_chunks(input_csv, chunksize), desc="Pivoting", unit="chunk", ncols=100):
        for _, row in chunk.iterrows():
            index_key = tuple(row[col] for col in index_cols)
            pivot_key = tuple(row[col] for col in pivot_cols)
            key = (index_key, pivot_key)
            val = row[value_col]
            if aggfunc == 'count':
                agg_dict[key] = agg_dict.get(key, 0) + 1
            elif aggfunc == 'sum':
                agg_dict[key] = agg_dict.get(key, 0) + val
            elif aggfunc == 'nunique':
                if key not in agg_dict:
                    agg_dict[key] = set()
                agg_dict[key].add(val)
            else:
                raise ValueError("Unsupported aggregation function")
    
    # Gather all unique index and pivot keys.
    index_keys = set()
    pivot_keys = set()
    for (idx_key, p_key) in agg_dict.keys():
        index_keys.add(idx_key)
        pivot_keys.add(p_key)
    index_keys = list(index_keys)
    pivot_keys = list(pivot_keys)
    
    # Build a header: index columns + one column per (flattened) pivot key.
    header = list(index_cols)
    pivot_header = ["_".join(map(str, pk)) for pk in pivot_keys]
    header.extend(pivot_header)
    
    # Build rows: for each unique index, for each pivot group, use the aggregated value.
    lookup = {}
    for (idx_key, p_key), agg_val in agg_dict.items():
        if aggfunc == 'nunique':
            agg_val = len(agg_val)
        lookup[(idx_key, p_key)] = agg_val
    
    result_rows = []
    for idx_key in index_keys:
        row_vals = list(idx_key)
        for p_key in pivot_keys:
            row_vals.append(lookup.get((idx_key, p_key), 0))
        result_rows.append(row_vals)
    
    if output_csv is None:
        output_csv = os.path.join(os.path.dirname(input_csv), f"pivot__{os.path.basename(input_csv)}")
    
    with open(output_csv, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(header)
        for row in result_rows:
            writer.writerow(row)
    
    return output_csv

# -------------------------
# 2. Process CSV: Remove Parentheses
# -------------------------

def process_csv_remove_parentheses(input_csv, columns, chunksize=10000, edit_in_place=True, output_csv=None):
    """
    Remove any text within parentheses from the specified columns.
    
    Parameters:
      input_csv    : Path to the input CSV.
      columns      : List of column names on which to remove text within parentheses.
      chunksize    : Number of rows to process per chunk.
      edit_in_place: If True, the original column is overwritten; otherwise, a new column is added.
      output_csv   : (Optional) Output CSV filename. If None, one is generated.
    
    Returns:
      The output CSV filename.
    """
    if output_csv is None:
        output_csv = os.path.join(os.path.dirname(input_csv), f"no_parentheses__{os.path.basename(input_csv)}")
    first_chunk = True
    reader = pd.read_csv(input_csv, chunksize=chunksize, low_memory=False)
    for chunk in tqdm(reader, desc="Removing Parentheses", unit="chunk", ncols=100):
        for col in columns:
            if col in chunk.columns:
                new_col = col if edit_in_place else f"__{col}"
                # Process the column as string.
                chunk[new_col] = chunk[col].astype(str).apply(lambda x: re.sub(r'\(.*?\)', '', x).rstrip())
                if not edit_in_place:
                    chunk.drop(columns=[col], inplace=True)
        mode = 'w' if first_chunk else 'a'
        header = first_chunk
        with open(output_csv, mode, newline='', encoding='utf-8') as f_out:
            chunk.to_csv(f_out, index=False, header=header)
        first_chunk = False
    return output_csv

# -------------------------
# 3. Generate Column Analytics
# -------------------------

def generate_column_analytics(input_csv, output_csv=None, chunksize=10000):
    """
    Compute basic analytics for each column (e.g. total rows, non-null count, unique count) 
    by processing the CSV in chunks.
    
    Parameters:
      input_csv  : Path to the input CSV.
      output_csv : (Optional) Output CSV filename for the analytics.
      chunksize  : Number of rows to process per chunk.
    
    Returns:
      The output CSV filename.
    """
    total_rows = count_rows(input_csv, chunksize)
    aggregated = {}
    
    for chunk in tqdm(read_csv_in_chunks(input_csv, chunksize), desc="Analyzing columns", unit="chunk", ncols=100):
        for col in chunk.columns:
            if col not in aggregated:
                aggregated[col] = {'total': 0, 'non_null': 0, 'null': 0, 'unique': set()}
            aggregated[col]['total'] += len(chunk)
            aggregated[col]['non_null'] += chunk[col].count()
            aggregated[col]['null'] += chunk[col].isnull().sum()
            aggregated[col]['unique'].update(chunk[col].dropna().unique())
    
    output_data = []
    for col, stats in aggregated.items():
        output_data.append({
            'column': col,
            'total': stats['total'],
            'non_null': stats['non_null'],
            'null': stats['null'],
            'unique_count': len(stats['unique'])
        })
    df_out = pd.DataFrame(output_data)
    
    if output_csv is None:
        output_csv = os.path.join(os.path.dirname(input_csv), f"col_analysis__{os.path.basename(input_csv)}")
    
    df_out.to_csv(output_csv, index=False, quoting=csv.QUOTE_ALL, escapechar='"')
    return output_csv

# -------------------------
# 4. Join Two Large CSVs
# -------------------------

def join_large_csvs(left_file, right_file, left_on, right_on, join_type='left', chunksize=50000, output_csv=None):
    """
    Join two CSV files using a streaming approach.
    
    Reads the left_file in chunks and merges each chunk with the (fully loaded) right_file.
    (It is assumed that the right_file is small enough to load entirely.)
    
    Parameters:
      left_file  : Path to the main (large) CSV file.
      right_file : Path to the CSV file to join.
      left_on    : Column name on the left file to join on.
      right_on   : Column name on the right file to join on.
      join_type  : Type of join (e.g. 'left', 'inner').
      chunksize  : Number of rows per chunk.
      output_csv : (Optional) Output CSV filename.
    
    Returns:
      The output CSV filename.
    """
    right_df = pd.read_csv(right_file)
    if output_csv is None:
        output_csv = os.path.join(os.path.dirname(left_file), f"joined__{os.path.basename(left_file)}")
    first_chunk = True
    reader = pd.read_csv(left_file, chunksize=chunksize, low_memory=False)
    for chunk in tqdm(reader, desc="Joining CSVs", unit="chunk", ncols=100):
        merged = pd.merge(chunk, right_df, left_on=left_on, right_on=right_on, how=join_type)
        mode = 'w' if first_chunk else 'a'
        header = first_chunk
        with open(output_csv, mode, newline='', encoding='utf-8') as f_out:
            merged.to_csv(f_out, index=False, header=header)
        first_chunk = False
    return output_csv

# -------------------------
# 5. Unique Filter (streaming)
# -------------------------

def unique_filter(input_csv, unique_cols, output_csv, chunksize=10000):
    """
    Filter the input CSV so that only unique rows (based on the specified columns)
    are written to the output CSV. Uses a streaming approach.

    If `unique_cols` is empty (or evaluates to False), the uniqueness is based on
    all columns in the row. In that case, a hash of all row values (joined by commas)
    is used as the uniqueness key to save memory.

    Parameters:
      input_csv  : Path to the input CSV.
      unique_cols: List of column names that define uniqueness. If empty, uniqueness
                   is computed on all columns.
      output_csv : Path to the output CSV.
      chunksize  : Number of rows to process per chunk.

    Returns:
      The output CSV filename.
    """
    seen = set()
    first_chunk = True
    reader = pd.read_csv(input_csv, chunksize=chunksize, low_memory=False)
    for chunk in tqdm(reader, desc="Filtering unique rows", unit="chunk", ncols=100):
        mask = []
        for idx, row in chunk.iterrows():
            if not unique_cols:
                key_str = ','.join(str(v) for v in row.values)
                key = hash(key_str)
            else:
                key = tuple(row[col] for col in unique_cols)
            if key in seen:
                mask.append(False)
            else:
                seen.add(key)
                mask.append(True)
        filtered_chunk = chunk[mask]
        if not filtered_chunk.empty:
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            with open(output_csv, mode, newline='', encoding='utf-8') as f_out:
                filtered_chunk.to_csv(f_out, index=False, header=header)
            first_chunk = False
    return output_csv

# -------------------------
# (Optional) Additional helper function: infer_dtypes
# -------------------------

def infer_dtypes(file_path, nrows=1000):
    """
    Infer the data types of the columns in a CSV by reading a small sample.
    """
    df_sample = pd.read_csv(file_path, nrows=nrows)
    dtypes = df_sample.dtypes.to_dict()
    return {col: str(dtype) for col, dtype in dtypes.items()}

def concatenate_csv_folder(input_folder, output_file, chunksize=10000):
    """
    Concatenates all CSV files in a folder into a single CSV file.
    
    Assumes all CSVs share the same header.
    
    Parameters:
      input_folder: Path to the folder containing CSV files.
      output_file : Path where the combined CSV will be saved.
      chunksize   : Number of rows per chunk to read.
      
    Returns:
      The output_file name.
    """
    csv_files = glob.glob(os.path.join(input_folder, '*.csv'))
    if not csv_files:
        raise ValueError("No CSV files found in folder: " + input_folder)
    first_file = True
    for file in tqdm(csv_files, desc="Concatenating CSV files", unit="file", ncols=100):
        print(f"Processing file: {file}")
        reader = pd.read_csv(file, chunksize=chunksize, low_memory=False)
        for chunk in tqdm(reader, desc=f"Processing chunks of {os.path.basename(file)}", unit="chunk", ncols=100):
            mode = 'w' if first_file else 'a'
            header = first_file
            with open(output_file, mode, newline='', encoding='utf-8') as fout:
                chunk.to_csv(fout, index=False, header=header)
            first_file = False
    return output_file
