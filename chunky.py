#!/usr/bin/env python3
"""
chunky.py – A minimal CSV transformation library using streaming/chunked processing.
This package includes functions to:
  • Count rows and stream CSVs in chunks.
  • Build a pivot table from a large CSV (with aggregations such as count, sum, nunique, and concatenation).
  • Filter CSV rows using a stateless filter function.
  • Join two CSV files using streaming with a custom progress bar for comparisons.
  • Other utilities: unique_filter, process_csv_remove_parentheses, generate_column_analytics, 
    concatenate_csv_folder, and rename_csv_header.
All functions output a CSV file and use progress bars that update based on row counts.
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
# Helper functions for chunking and row counting
# -------------------------

def count_rows(file_path, chunksize=10000):
    """Count the total number of rows in a CSV by processing it in chunks."""
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
    """Count rows using a streaming approach (returns a total row count)."""
    return count_rows(file_path, chunksize)

# -------------------------
# Custom Comparison Progress Bar for Joins
# -------------------------

class CustomComparisonTqdm(tqdm):
    @staticmethod
    def format_meter(n, total, elapsed, rate_fmt=None, postfix=None, ncols=None, **extra_kwargs):
        percentage = (n / total * 100) if total else 0
        formatted_percentage = f"{percentage:.1f}"
        if total is not None:
            return f"Processing comparisons: {humanize.intcomma(n)}/{humanize.intcomma(total)} comparisons " \
                   f"({formatted_percentage}%) [{tqdm.format_interval(elapsed)}]"
        else:
            return f"Processing comparisons: {humanize.intcomma(n)} comparisons " \
                   f"[{tqdm.format_interval(elapsed)}]"

# -------------------------
# 1. Streaming Pivot Table with Advanced Aggregation
# -------------------------

def streaming_pivot(
    input_csv,
    index_cols,
    pivot_cols,
    value_cols,
    aggfuncs,
    chunksize=10000,
    output_csv=None,
    concat_sep=", "
):
    """
    Build a pivot table from a large CSV file using streaming/chunked processing.
    
    Parameters:
      input_csv : Path to the input CSV.
      index_cols: List of column names to use as the pivot table's index.
                  (like "Rows" in an Excel pivot table)
      pivot_cols: List of column names to pivot on. (like "Columns" in Excel)
                  If empty, no extra pivoting occurs (one column per aggregator).
      value_cols: List of column names whose values are to be aggregated
                  (like "Values" fields in Excel pivot).
      aggfuncs  : Either a single string (applied to all value_cols) or a dictionary
                  mapping each value_col to the aggregator to use. Supported aggregator strings:
                    - 'count'     → increments an integer
                    - 'sum'       → sums float values
                    - 'nunique'   → collects a set of unique values
                    - 'concat'    → collects a list of values, joined by `concat_sep`
                    - 'countcat'  → collects all values in a Counter for frequency-based output
      chunksize : Number of rows to process per chunk.
      output_csv: (Optional) Path for the output CSV. If None, uses a default name.
      concat_sep: Separator string when aggregator == 'concat'.

    Returns:
      The output CSV filename.

    Notes:
      - The resulting columns will be named "<col>_<aggregator>_<pivotvals...>" 
        (or just "<col>_<aggregator>" if pivot_cols is empty).
      - This ensures that if you have multiple aggregations on the same column,
        (e.g. "count" and "countcat"), the resulting columns won't collide.
    """
    import csv
    import os
    import pandas as pd
    from tqdm import tqdm
    from collections import Counter

    # Count total rows so we can show progress
    total_rows = count_rows_in_chunks(input_csv, chunksize)

    # If aggfuncs is just a single aggregator, apply it to all value_cols
    if not isinstance(aggfuncs, dict):
        aggfuncs = {col: aggfuncs for col in value_cols}

    # Our aggregator structure: 
    #   agg_dict[(index_vals, pivot_vals)][col] = aggregator storage
    agg_dict = {}

    with tqdm(total=total_rows, desc="Pivoting rows", unit="row", ncols=100) as pbar:
        for chunk in read_csv_in_chunks(input_csv, chunksize):
            for _, row in chunk.iterrows():
                # Build the pivot key
                key_index = tuple(row[col] for col in index_cols)
                key_pivot = tuple(row[col] for col in pivot_cols) if pivot_cols else tuple()
                key = (key_index, key_pivot)

                # Initialize aggregator if new
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
                        elif func == 'countcat':
                            agg_dict[key][col] = Counter()
                        else:
                            raise ValueError(f"Unsupported aggregator: {func}")

                # Update aggregator with this row
                for col in value_cols:
                    func = aggfuncs[col]
                    val = row[col]
                    if func == 'count':
                        agg_dict[key][col] += 1
                    elif func == 'sum':
                        try:
                            agg_dict[key][col] += float(val)
                        except:
                            pass
                    elif func == 'nunique':
                        agg_dict[key][col].add(val)
                    elif func == 'concat':
                        agg_dict[key][col].append(str(val))
                    elif func == 'countcat':
                        agg_dict[key][col][str(val)] += 1
            pbar.update(len(chunk))

    # Collate unique index and pivot combinations
    unique_index = {k[0] for k in agg_dict.keys()}
    unique_pivot = {k[1] for k in agg_dict.keys()}
    unique_index = list(unique_index)
    unique_pivot = list(unique_pivot)

    # Build final pivot table columns
    # We embed aggregator name in the column to avoid collisions
    def aggregator_label(func_name):
        return str(func_name)

    # So final column name = "<value_col>_<aggregator>_<pivotvals...>"
    header = list(index_cols)
    pivot_col_names = []
    for p in unique_pivot:
        pivot_str = "_".join(map(str, p)) if p else ""
        for col in value_cols:
            func = aggfuncs[col]
            label = aggregator_label(func)
            if pivot_str:
                pivot_col_names.append(f"{col}_{label}_{pivot_str}")
            else:
                pivot_col_names.append(f"{col}_{label}")
    header.extend(pivot_col_names)

    # Build rows
    result_rows = []
    for idx_key in unique_index:
        row_vals = list(idx_key)  # Start with index col values
        for p in unique_pivot:
            key = (idx_key, p)
            if key in agg_dict:
                # gather aggregator results for each value_col
                for col in value_cols:
                    func = aggfuncs[col]
                    aggregator = agg_dict[key][col]
                    if func == 'nunique':
                        row_vals.append(len(aggregator))
                    elif func == 'concat':
                        row_vals.append(concat_sep.join(aggregator))
                    elif func == 'countcat':
                        # Sort by descending count
                        items = aggregator.most_common()
                        row_vals.append("; ".join(f"{val}({cnt})" for val, cnt in items))
                    else:
                        # e.g. 'count' or 'sum'
                        row_vals.append(aggregator)
            else:
                # if not present, fill with a default
                for col in value_cols:
                    func = aggfuncs[col]
                    if func in ['count', 'sum', 'nunique']:
                        row_vals.append(0)
                    else:
                        # e.g. 'concat', 'countcat'
                        row_vals.append("")
        result_rows.append(row_vals)

    # If no output_csv provided, make a default
    if output_csv is None:
        base = os.path.basename(input_csv)
        root, _ = os.path.splitext(base)
        output_csv = os.path.join(os.path.dirname(input_csv), f"pivot__{base}")

    # Write final CSV
    import csv
    with open(output_csv, 'w', newline='', encoding='utf-8') as fout:
        w = csv.writer(fout)
        w.writerow(header)
        for row_data in result_rows:
            w.writerow(row_data)

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
      filter_func: A function that takes a DataFrame chunk and returns a Boolean Series
                   (of the same length) indicating which rows to keep.
      chunksize : Number of rows to process per chunk.
    Returns:
      The output CSV filename.
    """
    first_chunk = True
    total = count_rows(input_csv, chunksize)
    with tqdm(total=total, desc="Filtering CSV", unit="row", ncols=100) as pbar:
        for chunk in read_csv_in_chunks(input_csv, chunksize):
            mask = filter_func(chunk)
            if not isinstance(mask, pd.Series) or mask.dtype != bool or len(mask) != len(chunk):
                raise ValueError("filter_func must return a Boolean Series of the same length as the chunk.")
            filtered = chunk.loc[mask]
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            with open(output_csv, mode, newline='', encoding='utf-8') as fout:
                filtered.to_csv(fout, index=False, header=header)
            first_chunk = False
            pbar.update(chunk.shape[0])
    return output_csv

# -------------------------
# 3. Unique Filter (streamed)
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
    total = count_rows(input_csv, chunksize)
    with tqdm(total=total, desc="Filtering unique rows", unit="row", ncols=100) as pbar:
        for chunk in read_csv_in_chunks(input_csv, chunksize):
            mask = []
            for idx, row in chunk.iterrows():
                if not unique_cols:
                    key = hash(','.join(str(v) for v in row.values))
                else:
                    key = tuple(row[col] for col in unique_cols)
                if key in seen:
                    mask.append(False)
                else:
                    seen.add(key)
                    mask.append(True)
            filtered_chunk = chunk.loc[mask]
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            with open(output_csv, mode, newline='', encoding='utf-8') as fout:
                filtered_chunk.to_csv(fout, index=False, header=header)
            first_chunk = False
            pbar.update(chunk.shape[0])
    return output_csv

# -------------------------
# 4. Join Large CSVs using a Custom Comparison Progress Bar
# -------------------------

def join_large_csvs(left_file, right_file, left_on, right_on, join_type='left', chunksize=50000, output_csv=None, suffixes=('_x', '_y')):
    """
    Join two CSV files using a streaming approach with a custom progress bar for comparisons.
    Reads the left_file in chunks and, for each left chunk, iterates through chunks of the right_file.
    Total comparisons are estimated as total_rows_left * total_rows_right.
    Parameters:
      left_file  : Path to the main (large) CSV file.
      right_file : Path to the CSV file to join.
      left_on    : Column name(s) on the left file to join on.
      right_on   : Column name(s) on the right file to join on.
      join_type  : Type of join (e.g., 'left', 'inner').
      chunksize  : Number of rows per chunk.
      output_csv : (Optional) Output CSV filename.
      suffixes   : Suffixes to append to overlapping columns.
    Returns:
      The output CSV filename.
    """
    suffixes = tuple(suffixes)
    total_rows_left = count_rows_in_chunks(left_file, chunksize)
    total_rows_right = count_rows_in_chunks(right_file, chunksize)
    total_comparisons = total_rows_left * total_rows_right
    if output_csv is None:
        input_csv_basename = os.path.basename(left_file)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = os.path.join(os.path.dirname(left_file), f"joined__{filename_without_ext}.csv")
    with open(output_csv, 'w', newline='', encoding='utf-8-sig') as f_output:
        writer = None
        with CustomComparisonTqdm(total=total_comparisons, desc='Processing comparisons', unit='comparison', ncols=100) as pbar:
            for left_chunk in read_csv_in_chunks(left_file, chunksize):
                for right_chunk in read_csv_in_chunks(right_file, chunksize):
                    # If suffixes are empty, drop overlapping columns (except the join key)
                    if suffixes == ('', ''):
                        overlapping = set(left_chunk.columns) & set(right_chunk.columns)
                        if right_on in overlapping:
                            overlapping.remove(right_on)
                        right_chunk = right_chunk.drop(columns=overlapping, errors='ignore')
                    df_chunk = pd.merge(left_chunk, right_chunk, how=join_type, left_on=left_on, right_on=right_on, suffixes=suffixes)
                    if writer is None:
                        df_chunk.to_csv(f_output, index=False)
                        writer = True
                    else:
                        df_chunk.to_csv(f_output, header=False, mode='a', index=False)
                    # Update progress: assume each left row is compared with all rows in the current right chunk.
                    pbar.update(left_chunk.shape[0] * right_chunk.shape[0])
    return output_csv

# -------------------------
# 5. Process CSV: Remove Parentheses
# -------------------------

def process_csv_remove_parentheses(input_csv, columns, chunksize=10000, edit_in_place=True, output_csv=None):
    """
    Remove any text within parentheses from the specified columns.
    Parameters:
      input_csv    : Path to the input CSV.
      columns      : List of column names to process.
      chunksize    : Number of rows to process per chunk.
      edit_in_place: If True, overwrite original column; otherwise create a new column.
      output_csv   : (Optional) Output CSV filename.
    Returns:
      The output CSV filename.
    """
    if output_csv is None:
        output_csv = os.path.join(os.path.dirname(input_csv), f"no_parentheses__{os.path.basename(input_csv)}")
    first_chunk = True
    for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
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

# -------------------------
# 6. Generate Column Analytics
# -------------------------

def generate_column_analytics(input_csv, output_csv=None, chunksize=10000):
    """
    Compute basic analytics (total rows, non-null count, unique count, etc.) for each column
    by processing the CSV in chunks. The progress bar is updated based on the number of rows processed.
    
    Parameters:
      input_csv  : Path to the input CSV.
      output_csv : (Optional) Output CSV filename.
      chunksize  : Number of rows per chunk.
    
    Returns:
      The output CSV filename.
    """
    total = count_rows(input_csv, chunksize)
    aggregated = {}
    with tqdm(total=total, desc="Analyzing columns", unit="row", ncols=100) as pbar:
        for chunk in read_csv_in_chunks(input_csv, chunksize):
            for col in chunk.columns:
                if col not in aggregated:
                    aggregated[col] = {'total': 0, 'non_null': 0, 'null': 0, 'unique': set()}
                aggregated[col]['total'] += len(chunk)
                aggregated[col]['non_null'] += chunk[col].count()
                aggregated[col]['null'] += chunk[col].isnull().sum()
                aggregated[col]['unique'].update(chunk[col].dropna().unique())
            pbar.update(len(chunk))
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

def generate_column_analytics_in_chunks(input_csv, output_csv=None, chunksize=10000,
                                        max_value_length=5000, long_value_handling='truncate',
                                        show_unique_values=True, show_unique_counts=True,
                                        uniq_value_mode='efficient', nonnull_threshold=0.96,
                                        efficient_mode_multiplier=5):
    total_rows = count_rows(input_csv)

    aggregated_analytics = {}  # Use a dictionary to store aggregated analytics
    unique_values_sets = {}  # Initialize the unique_values_sets dictionary

    result = pd.DataFrame()

    with tqdm(total=total_rows, desc='Processing chunks', unit=' rows', ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False):
            chunk_stats = []

            for column_name in chunk.columns:
                col_data = chunk[column_name]
                if column_name not in unique_values_sets:
                    unique_values_sets[column_name] = set()
                unique_values_sets[column_name].update(col_data.dropna().unique())

                if pd.api.types.is_numeric_dtype(col_data):
                    col_stats = {
                        'column_name': column_name,
                        'mean': col_data.mean(),
                        'median': col_data.median(),
                        'std': col_data.std(),
                        'min': col_data.min(),
                        'max': col_data.max(),
                        '25_percentile': col_data.quantile(0.25),
                        '75_percentile': col_data.quantile(0.75),
                        'unique': col_data.nunique(),
                        'non_null': col_data.count(),
                        'null': col_data.isnull().sum(),
                        'percent_non_null': col_data.count() / total_rows,
                        'percent_unique': col_data.nunique() / total_rows,
                        'mode': col_data.mode().iloc[0] if not col_data.mode().empty else None
                    }
                else:
                    col_stats = {
                        'column_name': column_name,
                        'unique': col_data.nunique(),
                        'non_null': col_data.count(),
                        'null': col_data.isnull().sum(),
                        'mode': col_data.mode().iloc[0] if not col_data.mode().empty else None,
                        'percent_non_null': col_data.count() / total_rows,
                        'percent_unique': col_data.nunique() / total_rows
                    }

                    def stringify_values(value_counts):
                        str_value_counts = {}
                        for k, v in value_counts.items():
                            str_value_counts[str(k)] = v
                        return str_value_counts

                    if show_unique_values:
                        value_counts = col_data.value_counts()
                        str_value_counts = stringify_values(value_counts)

                        if show_unique_counts:
                            col_stats['unique_values'] = str_value_counts
                        else:
                            col_stats['unique_values'] = list(str_value_counts.keys())

                        serialized_unique_values = json.dumps(col_stats['unique_values'], default=str)

                        if max_value_length is not None and len(serialized_unique_values) > max_value_length:
                            if long_value_handling == 'truncate':
                                col_stats['unique_values'] = serialized_unique_values[:max_value_length]
                            elif long_value_handling == 'horizontal':
                                serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                                                  in
                                                                  range(0, len(serialized_unique_values),
                                                                        max_value_length)]
                                col_stats['unique_values'] = {}
                                for idx, part in enumerate(serialized_unique_values_parts):
                                    new_key = f"unique_values_part[{idx}]"
                                    col_stats['unique_values'][new_key] = part
                            elif long_value_handling == 'explode':
                                serialized_unique_values_parts = [serialized_unique_values[i:i + max_value_length] for i
                                                                  in
                                                                  range(0, len(serialized_unique_values),
                                                                        max_value_length)]
                                col_stats['unique_values'] = []
                                for part in serialized_unique_values_parts:
                                    col_stats['unique_values'].append(part)

                        if uniq_value_mode == 'efficient':
                            if col_stats['percent_non_null'] >= nonnull_threshold:
                                uniq_count_threshold = efficient_mode_multiplier * col_data.nunique()
                                if len(col_stats['unique_values']) > uniq_count_threshold:
                                    col_stats['unique_values'] = 'Exceeded count threshold'

                chunk_stats.append(col_stats)

            result = result.append(chunk_stats, ignore_index=True)
            pbar.update(chunk.shape[0])

    if output_csv is None:
        input_csv_basename = os.path.basename(input_csv)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = f'col_analysis__{filename_without_ext}.csv'

    result.to_csv(output_csv, index=False, escapechar='"', quoting=csv.QUOTE_ALL)
    return output_csv

# -------------------------
# 7. Concatenate CSV Folder
# -------------------------

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
    for file in glob.glob(os.path.join(input_folder, '*.csv')):
        print(f"Processing file: {file}")
        for chunk in pd.read_csv(file, chunksize=chunksize, low_memory=False):
            mode = 'w' if first_file else 'a'
            header = first_file
            with open(output_file, mode, newline='', encoding='utf-8') as fout:
                chunk.to_csv(fout, index=False, header=header)
            first_file = False
    return output_file

# -------------------------
# 8. Rename CSV Header (Streaming)
# -------------------------

def rename_csv_header(input_csv, output_csv, transformations=None, delimiter=",", chunksize=10000):
    """
    Rename header fields in a CSV file according to provided regex transformations.
    This version uses a row-based streaming approach so that the progress bar is updated by row count.
    Parameters:
      input_csv      : Path to the input CSV.
      output_csv     : Path to the output CSV.
      transformations: Dictionary mapping regex patterns to replacement strings or callables.
      delimiter      : Field delimiter (default is comma).
      chunksize      : Number of rows per chunk (for streaming the file body).
    Returns:
      The output CSV filename.
    """
    # Read the header using the csv module for minimal memory usage.
    with open(input_csv, 'r', encoding='utf-8') as fin:
        reader = csv.reader(fin, delimiter=delimiter)
        headers = next(reader)
    
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
    
    # Write new header.
    with open(output_csv, 'w', newline='', encoding='utf-8') as fout:
        writer = csv.writer(fout, delimiter=delimiter)
        writer.writerow(new_headers)
    
    # Compute total rows minus one (since header is already processed).
    total = count_rows(input_csv, chunksize) - 1
    with tqdm(total=total, desc="Renaming header & copying rows", unit="row", ncols=100) as pbar:
        # Stream the rest of the CSV, skipping the header line.
        for chunk in pd.read_csv(input_csv, chunksize=chunksize, low_memory=False, skiprows=1):
            with open(output_csv, 'a', newline='', encoding='utf-8') as fout:
                chunk.to_csv(fout, index=False, header=False)
            pbar.update(chunk.shape[0])
    return output_csv

def select_columns(input_csv, columns, output_csv, chunksize=10000):
    """
    Create a new CSV file containing only the specified columns from the input CSV,
    processing the file in chunks to keep memory usage low.
    Parameters:
      input_csv : Path to the input CSV.
      columns   : List of column names to select.
      output_csv: Path to the output CSV.
      chunksize : Number of rows per chunk.
    Returns:
      The output CSV filename.
    """
    # Count total rows to set up the progress bar.
    total = count_rows(input_csv, chunksize)
    
    first_chunk = True
    with tqdm(total=total, desc="Selecting columns", unit="row", ncols=100) as pbar:
        for chunk in read_csv_in_chunks(input_csv, chunksize):
            # Select only the specified columns.
            selected_chunk = chunk[columns]
            # Write to output file: header only for the first chunk.
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            with open(output_csv, mode, newline='', encoding='utf-8') as fout:
                selected_chunk.to_csv(fout, index=False, header=header)
            first_chunk = False
            pbar.update(chunk.shape[0])
    
    return output_csv

def add_new_columns(input_csv, output_csv, new_columns, chunksize=10000):
    """
    Create new columns in a CSV file using functions provided in new_columns dict.
    
    Parameters:
      input_csv   : Path to the input CSV.
      output_csv  : Path to the output CSV.
      new_columns : Dict mapping new column names to functions that take a row (pd.Series) and return a value.
      chunksize   : Number of rows per chunk.
    
    Returns:
      The output CSV filename.
    """
    total = count_rows(input_csv, chunksize)
    first_chunk = True
    with tqdm(total=total, desc="Adding new columns", unit="row", ncols=100) as pbar:
        for chunk in read_csv_in_chunks(input_csv, chunksize):
            for new_col, func in new_columns.items():
                chunk[new_col] = chunk.apply(func, axis=1)
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            with open(output_csv, mode, newline='', encoding='utf-8') as fout:
                chunk.to_csv(fout, index=False, header=header)
            first_chunk = False
            pbar.update(chunk.shape[0])
    return output_csv

def infer_dtypes(file_path, nrows=1000):
    """
    Infer the data types of the columns in a CSV by reading a small sample.
    """
    df_sample = pd.read_csv(file_path, nrows=nrows)
    dtypes = df_sample.dtypes.to_dict()
    return {col: str(dtype) for col, dtype in dtypes.items()}

def trim_csv(input_csv, output_csv, row_count, chunksize=100):
    """
    Trim down the input CSV to the specified number of rows using streaming and save it to the output CSV.
    
    Parameters:
      input_csv  : Path to the input CSV.
      output_csv : Path to the output CSV.
      row_count  : Number of rows to retain in the trimmed CSV.
      chunksize  : Number of rows per chunk.
    
    Returns:
      The output CSV filename.
    """
    total_rows_written = 0
    first_chunk = True

    with tqdm(total=row_count, desc="Trimming rows", unit="row", ncols=100) as pbar:
        for chunk in pd.read_csv(input_csv, chunksize=chunksize):
            if total_rows_written + len(chunk) > row_count:
                chunk = chunk.head(row_count - total_rows_written)
            
            mode = 'w' if first_chunk else 'a'
            header = first_chunk
            with open(output_csv, mode, newline='', encoding='utf-8') as fout:
                chunk.to_csv(fout, index=False, header=header)
            
            total_rows_written += len(chunk)
            pbar.update(len(chunk))
            
            if total_rows_written >= row_count:
                break
            
            first_chunk = False
            
    return output_csv

def join_large_csvs_2(left_file, right_file, left_on, right_on, join_type='left', chunksize=50000, output_csv=None, suffixes=('_x', '_y')):
    suffixes = tuple(suffixes)  # Convert list to tuple

    # Get the total number of rows in the left and right CSV files for progress bars
    total_rows_left = count_rows_in_chunks(left_file, chunksize)
    total_rows_right = count_rows_in_chunks(right_file, chunksize)

    # Calculate the total number of comparisons
    total_comparisons = total_rows_left * total_rows_right

    if output_csv is None:
        input_csv_basename = os.path.basename(left_file)
        filename_without_ext = os.path.splitext(input_csv_basename)[0]
        output_csv = os.path.join(os.path.dirname(left_file), f"joined__{filename_without_ext}.csv")

    # Open the output CSV file
    with open(output_csv, 'w', encoding='utf-8-sig') as f_output:
        writer = None

        # Initialize the progress bar for total comparisons
        with CustomComparisonTqdm(total=total_comparisons, desc='Processing comparisons', unit='comparisons',
                                  ncols=100) as pbar:
            # Read the left csv file in chunks
            for left_chunk in read_csv_in_chunks(left_file, chunksize):
                # Read the right csv file in chunks
                for right_chunk in read_csv_in_chunks(right_file, chunksize):
                    # If both suffixes are empty, identify overlapping columns (excluding the merge column)
                    # to be dropped from the right dataframe
                    if suffixes == ('', ''):
                        overlapping_columns = set(left_chunk.columns) & set(right_chunk.columns)
                        if right_on in overlapping_columns:  # Only attempt to remove if it exists
                            overlapping_columns.remove(right_on)  # Ensure the merge column is not dropped
                        right_chunk = right_chunk.drop(columns=overlapping_columns, errors='ignore')

                    df_chunk = pd.merge(left_chunk, right_chunk, how=join_type, left_on=left_on, right_on=right_on,
                                        suffixes=suffixes)

                    # Write the chunk to the output CSV file
                    if writer is None:
                        # If this is the first chunk, write the header and the data
                        df_chunk.to_csv(f_output, index=False)
                        writer = True
                    else:
                        # If this is not the first chunk, do not write the header again
                        df_chunk.to_csv(f_output, header=False, mode='a', index=False)

                    # Update the progress bar based on the right side chunksize
                    pbar.update(chunksize * len(left_chunk))

    # Return the output CSV file name
    return output_csv

def left_join_large_csvs_small_right(
    left_file,
    right_file,
    left_on,
    right_on,
    join_type='left',
    chunksize=50000,
    output_csv=None,
    suffixes=('_x', '_y'),
    encoding='utf-8-sig'
):
    """
    Join two CSV files by reading the ENTIRE right file into memory (assuming it's small)
    and streaming the left file in chunks. This is similar to an Excel VLOOKUP or
    typical merge with a small 'lookup' table.

    Parameters:
      left_file  : Path to the main (large) CSV file (we read it in chunks).
      right_file : Path to the smaller CSV file (read in full).
      left_on    : Column name(s) on the left file to join on.
      right_on   : Column name(s) on the right file to join on.
      join_type  : Type of join (e.g., 'left', 'inner'). Default is 'left'.
      chunksize  : Number of rows per chunk for the left file.
      output_csv : (Optional) Output CSV filename. If None, a default is generated.
      suffixes   : Suffixes to append to overlapping columns.
      encoding   : Encoding used when reading/writing CSV (default 'utf-8-sig').

    Returns:
      The output CSV filename.
    """

    import os
    import pandas as pd
    from tqdm import tqdm

    # If no output file is provided, create one from the left file's name.
    if output_csv is None:
        left_basename = os.path.basename(left_file)
        left_stem, left_ext = os.path.splitext(left_basename)
        output_csv = os.path.join(
            os.path.dirname(left_file),
            f"joined_smallright__{left_stem}.csv"
        )

    # Read the entire right CSV into memory (small enough).
    right_df = pd.read_csv(right_file, encoding=encoding, low_memory=False)

    # Count rows in the left file for the progress bar.
    total_rows_left = count_rows_in_chunks(left_file, chunksize)

    # Prepare to write the output CSV
    with open(output_csv, 'w', newline='', encoding=encoding) as f_out:
        writer = None

        with tqdm(total=total_rows_left, desc="Joining CSV (small right)", unit="row", ncols=100) as pbar:
            # Read the left file in chunks
            for left_chunk in read_csv_in_chunks(left_file, chunksize):
                # Merge with the in-memory right_df
                df_merged = pd.merge(
                    left_chunk,
                    right_df,
                    how=join_type,
                    left_on=left_on,
                    right_on=right_on,
                    suffixes=suffixes
                )
                # If it's the first chunk, write the header. Otherwise, append without header.
                if writer is None:
                    df_merged.to_csv(f_out, index=False, header=True)
                    writer = True
                else:
                    df_merged.to_csv(f_out, index=False, header=False)
                # Update the progress bar by the number of rows in this left chunk.
                pbar.update(left_chunk.shape[0])

    return output_csv
