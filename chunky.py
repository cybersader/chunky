#!/usr/bin/env python3
"""
chunky.py – A minimal CSV transformation library using streaming/chunked processing.

This package includes functions to:
  • Count rows and stream CSVs in chunks.
  • Build a pivot table from a large CSV (with aggregations such as count, sum, nunique, and concatenation).
  • Filter CSV rows using a stateless filter function.
  • (Other utility functions: join_large_csvs, unique_filter, process_csv_remove_parentheses, 
    generate_column_analytics, and concatenate_csv_folder.)

All functions output a CSV file and use progress bars that update using the actual row counts.
"""

import os
import re
import csv
import tempfile
import shutil
import glob
import pandas as pd
from tqdm import tqdm
import humanize  # used for pretty progress bar rates

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

def count_rows_in_chunks(file_path, chunksize=10000):
    """Count rows using a progress bar that updates by row count."""
    total = count_rows(file_path, chunksize)
    current = 0
    for chunk in read_csv_in_chunks(file_path, chunksize):
        current += chunk.shape[0]
    return current

# -------------------------
# 1. Streaming pivot table
# -------------------------

def streaming_pivot(input_csv, index_cols, pivot_cols, value_cols, aggfuncs, chunksize=10000,
                    output_csv=None, concat_sep=", "):
    """
    Build a pivot table from a large CSV file using streaming/chunked processing.
    
    Parameters:
      input_csv : Path to the input CSV.
      index_cols: List of column names to use as the pivot table's index.
      pivot_cols: List of column names to pivot on.
      value_cols: List of column names whose values are to be aggregated.
      aggfuncs  : Either a single string (applied to all value columns) or a dictionary mapping each value column
                 to an aggregation function. Supported functions are:
                   - 'count'
                   - 'sum'
                   - 'nunique'
                   - 'concat'  (to join values into a single string)
      chunksize : Number of rows to process per chunk.
      output_csv: (Optional) Path for the output CSV. If None, one is generated.
      concat_sep: Separator string used when concatenating values for 'concat' aggregation.
    
    Returns:
      The output CSV filename.
    """
    total_rows = count_rows_in_chunks(input_csv, chunksize)
    
    # Normalize aggfuncs to a dictionary
    if not isinstance(aggfuncs, dict):
        aggfuncs = {col: aggfuncs for col in value_cols}
    
    # Initialize aggregation dictionary.
    # Key: (tuple of index values, tuple of pivot values)
    # Value: dictionary mapping each value column to its aggregated value.
    agg_dict = {}
    
    with tqdm(total=total_rows, desc="Pivoting rows", unit="row", ncols=100) as pbar:
        for chunk in read_csv_in_chunks(input_csv, chunksize):
            for _, row in chunk.iterrows():
                key_index = tuple(row[col] for col in index_cols)
                key_pivot = tuple(row[col] for col in pivot_cols)
                key = (key_index, key_pivot)
                if key not in agg_dict:
                    agg_dict[key] = {}
                    for col in value_cols:
                        func = aggfuncs[col]
                        if func == 'count':
                            agg_dict[key][col] = 0
                        elif func == 'sum':
                            agg_dict[key][col] = 0
                        elif func == 'nunique':
                            agg_dict[key][col] = set()
                        elif func == 'concat':
                            agg_dict[key][col] = []
                        else:
                            raise ValueError(f"Unsupported aggregation function: {func}")
                for col in value_cols:
                    func = aggfuncs[col]
                    value = row[col]
                    if func == 'count':
                        agg_dict[key][col] += 1
                    elif func == 'sum':
                        try:
                            agg_dict[key][col] += float(value)
                        except Exception:
                            pass
                    elif func == 'nunique':
                        agg_dict[key][col].add(value)
                    elif func == 'concat':
                        agg_dict[key][col].append(str(value))
            pbar.update(chunk.shape[0])
    
    # Determine unique index and pivot keys.
    unique_index = set()
    unique_pivot = set()
    for (idx_key, p_key) in agg_dict.keys():
        unique_index.add(idx_key)
        unique_pivot.add(p_key)
    unique_index = list(unique_index)
    unique_pivot = list(unique_pivot)
    
    # Build header: index columns + one column per (pivot, value_col) combination.
    header = list(index_cols)
    pivot_col_names = []
    for p in unique_pivot:
        pivot_str = "_".join(map(str, p))
        for col in value_cols:
            pivot_col_names.append(f"{col}_{pivot_str}")
    header.extend(pivot_col_names)
    
    # Build result rows.
    result_rows = []
    for idx_key in unique_index:
        row_values = list(idx_key)
        for p in unique_pivot:
            key = (idx_key, p)
            if key in agg_dict:
                values = []
                for col in value_cols:
                    func = aggfuncs[col]
                    agg_val = agg_dict[key][col]
                    if func == 'nunique':
                        agg_val = len(agg_val)
                    elif func == 'concat':
                        agg_val = concat_sep.join(agg_val)
                    values.append(agg_val)
                row_values.extend(values)
            else:
                for col in value_cols:
                    func = aggfuncs[col]
                    if func in ['count', 'sum', 'nunique']:
                        row_values.append(0)
                    elif func == 'concat':
                        row_values.append("")
        result_rows.append(row_values)
    
    if output_csv is None:
        output_csv = os.path.join(os.path.dirname(input_csv), f"pivot__{os.path.basename(input_csv)}")
    
    with open(output_csv, 'w', newline='', encoding='utf-8') as fout:
        writer = csv.writer(fout)
        writer.writerow(header)
        for r in result_rows:
            writer.writerow(r)
    
    return output_csv

# -------------------------
# 2. CSV Filter (streamed)
# -------------------------

def filter_csv(input_csv, output_csv, filter_func, chunksize=10000):
    """
    Filter rows in a CSV file using a stateless filter function.

    Parameters:
      input_csv : Path to the input CSV.
      output_csv: Path to the output CSV.
      filter_func: A function that takes a DataFrame chunk and returns a boolean Series (of the same length)
                   indicating which rows to keep.
      chunksize : Number of rows to process per chunk.

    Returns:
      The output CSV filename.
    """
    first_chunk = True
    total = count_rows(input_csv, chunksize)
    with tqdm(total=total, desc="Filtering CSV", unit="row", ncols=100) as pbar:
        for chunk in read_csv_in_chunks(input_csv, chunksize):
            mask = filter_func(chunk)
            filtered = chunk[mask]
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            with open(output_csv, mode, newline='', encoding='utf-8') as fout:
                filtered.to_csv(fout, index=False, header=header)
            first_chunk = False
            pbar.update(chunk.shape[0])
    return output_csv

# -------------------------
# Other utility functions
# -------------------------

def unique_filter(input_csv, unique_cols, output_csv, chunksize=10000):
    """
    Filter the input CSV so that only unique rows (based on specified columns)
    are written to the output CSV using a streaming approach.

    If unique_cols is empty (or False), uniqueness is computed on all columns by hashing
    the comma-joined row values.

    Parameters:
      input_csv  : Path to the input CSV.
      unique_cols: List of column names that define uniqueness (if empty, use all columns).
      output_csv : Path to the output CSV.
      chunksize  : Number of rows per chunk.

    Returns:
      The output CSV filename.
    """
    seen = set()
    first_chunk = True
    for chunk in tqdm(pd.read_csv(input_csv, chunksize=chunksize, low_memory=False),
                      desc="Filtering unique rows", unit="row", ncols=100):
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
        mode = 'w' if first_chunk else 'a'
        header = first_chunk
        with open(output_csv, mode, newline='', encoding='utf-8') as fout:
            filtered_chunk.to_csv(fout, index=False, header=header)
        first_chunk = False
    return output_csv

def join_large_csvs(left_file, right_file, left_on, right_on, join_type='left', chunksize=50000, output_csv=None):
    """
    Join two CSV files using a streaming approach.

    Reads the left_file in chunks and merges each chunk with the fully loaded right_file.
    (Assumes right_file is small enough to load entirely.)

    Parameters:
      left_file  : Path to the main (large) CSV file.
      right_file : Path to the CSV file to join.
      left_on    : Column name(s) on the left file to join on.
      right_on   : Column name(s) on the right file to join on.
      join_type  : Type of join (e.g., 'left', 'inner').
      chunksize  : Number of rows per chunk.
      output_csv : (Optional) Output CSV filename.

    Returns:
      The output CSV filename.
    """
    right_df = pd.read_csv(right_file)
    if output_csv is None:
        output_csv = os.path.join(os.path.dirname(left_file), f"joined__{os.path.basename(left_file)}")
    first_chunk = True
    for chunk in tqdm(pd.read_csv(left_file, chunksize=chunksize, low_memory=False),
                      desc="Joining CSVs", unit="row", ncols=100):
        merged = pd.merge(chunk, right_df, left_on=left_on, right_on=right_on, how=join_type)
        mode = 'w' if first_chunk else 'a'
        header = first_chunk
        with open(output_csv, mode, newline='', encoding='utf-8') as fout:
            merged.to_csv(fout, index=False, header=header)
        first_chunk = False
    return output_csv

def process_csv_remove_parentheses(input_csv, columns, chunksize=10000, edit_in_place=True, output_csv=None):
    """
    Remove any text within parentheses from the specified columns.

    Parameters:
      input_csv    : Path to the input CSV.
      columns      : List of column names to process.
      chunksize    : Number of rows per chunk.
      edit_in_place: If True, overwrite original column; otherwise create a new column.
      output_csv   : (Optional) Output CSV filename.

    Returns:
      The output CSV filename.
    """
    if output_csv is None:
        output_csv = os.path.join(os.path.dirname(input_csv), f"no_parentheses__{os.path.basename(input_csv)}")
    first_chunk = True
    for chunk in tqdm(pd.read_csv(input_csv, chunksize=chunksize, low_memory=False),
                      desc="Removing Parentheses", unit="row", ncols=100):
        for col in columns:
            if col in chunk.columns:
                new_col = col if edit_in_place else f"__{col}"
                chunk[new_col] = chunk[col].astype(str).apply(lambda x: re.sub(r'\(.*?\)', '', x).rstrip())
                if not edit_in_place:
                    chunk.drop(columns=[col], inplace=True)
        mode = 'w' if first_chunk else 'a'
        header = first_chunk
        with open(output_csv, mode, newline='', encoding='utf-8') as fout:
            chunk.to_csv(fout, index=False, header=header)
        first_chunk = False
    return output_csv

def generate_column_analytics(input_csv, output_csv=None, chunksize=10000):
    """
    Compute basic analytics (total rows, non-null count, unique count, etc.) for each column
    by processing the CSV in chunks.

    Parameters:
      input_csv  : Path to the input CSV.
      output_csv : (Optional) Output CSV filename.
      chunksize  : Number of rows per chunk.

    Returns:
      The output CSV filename.
    """
    total = count_rows(input_csv, chunksize)
    aggregated = {}
    for chunk in tqdm(read_csv_in_chunks(input_csv, chunksize), desc="Analyzing columns", unit="row", ncols=100):
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

def concatenate_csv_folder(input_folder, output_file, chunksize=10000):
    """
    Concatenate all CSV files in a folder into one CSV file.
    
    Assumes all CSVs share the same header.

    Parameters:
      input_folder: Path to the folder containing CSV files.
      output_file : Path to save the combined CSV.
      chunksize   : Number of rows per chunk.

    Returns:
      The output_file name.
    """
    csv_files = glob.glob(os.path.join(input_folder, '*.csv'))
    if not csv_files:
        raise ValueError("No CSV files found in folder: " + input_folder)
    first_file = True
    for file in tqdm(csv_files, desc="Concatenating CSV files", unit="file", ncols=100):
        print(f"Processing file: {file}")
        for chunk in tqdm(pd.read_csv(file, chunksize=chunksize, low_memory=False),
                          desc=f"Processing chunks of {os.path.basename(file)}", unit="row", ncols=100):
            mode = 'w' if first_file else 'a'
            header = first_file
            with open(output_file, mode, newline='', encoding='utf-8') as fout:
                chunk.to_csv(fout, index=False, header=header)
            first_file = False
    return output_file

def rename_csv_header(input_csv, output_csv, transformations=None, delimiter=",", chunk_size=8192):
    """
    Rename header fields in a CSV file according to provided regex transformations.

    Parameters:
      input_csv      : Path to the input CSV.
      output_csv     : Path to the output CSV.
      transformations: Dictionary mapping regex patterns to replacement strings or callables.
      delimiter      : Field delimiter (default is comma).
      chunk_size     : Bytes per chunk when streaming the remainder of the file.

    Returns:
      The output CSV filename.
    """
    total_size = os.path.getsize(input_csv)
    with open(input_csv, 'r', encoding='utf-8') as fin, \
         open(output_csv, 'w', encoding='utf-8', newline='') as fout:
        
        header_line = fin.readline().rstrip("\r\n")
        headers = header_line.split(delimiter)
        
        new_headers = []
        for h in headers:
            h_clean = h
            if transformations:
                for pattern, replacement in transformations.items():
                    new_val = re.sub(pattern, replacement, h_clean)
                    if new_val != h_clean:
                        h_clean = new_val
                        break
            new_headers.append(h_clean)
        
        new_header_line = delimiter.join(new_headers) + "\n"
        fout.write(new_header_line)
        
        pos = fin.tell()
        remaining_bytes = total_size - pos
        with tqdm(total=remaining_bytes, desc="Copying CSV data", unit="byte", ncols=100) as pbar:
            while True:
                chunk = fin.read(chunk_size)
                if not chunk:
                    break
                fout.write(chunk)
                pbar.update(len(chunk))
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
