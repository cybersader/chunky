# Chunky CSV

**Chunky CSV** is a minimal CSV transformation library that processes large CSV files in chunks so that your computer’s memory is never overwhelmed. Built with streaming techniques and enhanced with progress bars using [tqdm](https://github.com/tqdm/tqdm), Chunky CSV offers a suite of functions to pivot data, clean and transform columns, generate column analytics, join large CSVs, and filter unique rows—all while writing the results directly to CSV files.

Enjoy processing your CSVs in a memory‑friendly way with Chunky CSV!

---

## Features

- **Streamed Processing:** Read and process CSV files in small chunks to keep memory usage low.
- **Pivot Table Generation:** Create pivot tables (with aggregation functions like count, sum, and nunique) from large datasets.
- **Data Cleaning:** Easily remove unwanted text (e.g. text in parentheses) from specified columns.
- **Column Analytics:** Generate per‑column statistics (total, non-null, unique count, etc.) without loading the entire dataset.
- **CSV Join:** Merge two CSV files by streaming through the larger file while keeping the smaller one in memory.
- **Unique Filtering:** Filter out duplicate rows based on one or more key columns.
- **Progress Bars:** All functions are equipped with tqdm-based progress bars for real‑time feedback during processing.

---

## Installation

Chunky CSV requires Python 3 and the following Python packages:

- [pandas](https://pandas.pydata.org/)
- [tqdm](https://github.com/tqdm/tqdm)
- [humanize](https://github.com/jmoiron/humanize) (optional, for pretty progress rate formatting)

You can install the dependencies with pip:

```bash
pip install pandas tqdm humanize
```

Simply copy the csv_lib.py file into your project directory to start using Chunky CSV.

## Usage
Import the library in your Python scripts and call the desired functions. Here are a few examples:

### Joining Two CSV Files

```python
import csv_lib

# Join two CSV files on a common key

joined_csv = csv_lib.join_large_csvs(
    left_file='data/main.csv',
    right_file='data/join.csv',
    left_on='id',
    right_on='id',
    join_type='left'
)
print(f"Joined CSV saved to: {joined_csv}")
```

### Creating a Pivot Table

```python
import csv_lib

# Generate a pivot table aggregating the 'amount' column by 'id' and 'category'
pivot_csv = csv_lib.streaming_pivot_table(
    input_csv=joined_csv,
    index_cols=['id'],
    pivot_cols=['category'],
    value_col='amount',
    aggfunc='sum'
)
print(f"Pivot table CSV saved to: {pivot_csv}")
```

### Removing Parentheses from Specific Columns 

```python
import csv_lib

# Remove text within parentheses from the 'description' column
clean_csv = csv_lib.process_csv_remove_parentheses(
    input_csv=pivot_csv,
    columns=['description'],
    edit_in_place=True
)
print(f"Cleaned CSV saved to: {clean_csv}")
```

### Generating Column Analytics

```python
import csv_lib

# Generate analytics for each column in the cleaned CSV
analytics_csv = csv_lib.generate_column_analytics(input_csv=clean_csv)
print(f"Column analytics CSV saved to: {analytics_csv}")
```

### Filtering Unique Rows

```python
import csv_lib

# Filter the CSV so that only unique rows (based on 'id') are output
unique_csv = csv_lib.unique_filter(
    input_csv=analytics_csv,
    unique_cols=['id'],
    output_csv='unique_output.csv'
)
print(f"Unique rows CSV saved to: {unique_csv}")
```

## API Reference
`streaming_pivot_table(input_csv, index_cols, pivot_cols, value_col, aggfunc='count', chunksize=10000, output_csv=None)`
Creates a pivot table from a large CSV by aggregating values (e.g. count, sum, nunique) without loading the entire dataset.

`process_csv_remove_parentheses(input_csv, columns, chunksize=10000, edit_in_place=True, output_csv=None)`
Removes text within parentheses from the specified columns.

`generate_column_analytics(input_csv, output_csv=None, chunksize=10000)`
Computes per‑column statistics (total, non-null, unique count, etc.) by processing the CSV in chunks.

`join_large_csvs(left_file, right_file, left_on, right_on, join_type='left', chunksize=50000, output_csv=None)`
Joins two CSV files by streaming through the larger file and merging with the smaller file.

`unique_filter(input_csv, unique_cols, output_csv, chunksize=10000)`
Writes only unique rows (based on specified columns) to the output CSV.

For a full list of functions and more details, see the source code in csv_lib.py.

## Contributions

Contributions, improvements, and suggestions are welcome! Feel free to open issues or submit pull requests on GitHub.

