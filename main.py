import csv_lib

# Join two CSV files:
joined = csv_lib.join_large_csvs('data/main.csv', 'data/join.csv', left_on='id', right_on='id', join_type='left')

# Create a pivot table (aggregating the “amount” column by id and category):
pivoted = csv_lib.streaming_pivot_table(joined, index_cols=['id'], pivot_cols=['category'],
                                         value_col='amount', aggfunc='sum')

# Remove parentheses from the "description" column:
clean_csv = csv_lib.process_csv_remove_parentheses(pivoted, columns=['description'], edit_in_place=True)

# Generate analytics on the cleaned CSV:
analytics_csv = csv_lib.generate_column_analytics(clean_csv)

# Filter the CSV so that only unique rows (based on a key column) are retained:
unique_csv = csv_lib.unique_filter(analytics_csv, unique_cols=['id'], output_csv='unique_output.csv')
