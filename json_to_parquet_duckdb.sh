#!/bin/bash

# Check if a filename was provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <jsonl_file> [output_name]"
    exit 1
fi

# Get the input file
input_file=$1

# Set default output name or use provided name
if [ -z "$2" ]; then
    # Remove extension and use base filename
    output_name=$(basename "$input_file" .jsonl)
else
    output_name=$2
fi

# Run duckdb commands
duckdb -c "

-- Create a table from the JSONL file
CREATE TABLE temp_table AS
  SELECT * FROM read_json('$input_file', union_by_name=true, maximum_object_size=256000000);

-- Export to Parquet format
COPY temp_table TO '${output_name}.parquet' (FORMAT PARQUET);

-- Drop the temporary table
DROP TABLE temp_table;
"

echo "Converted $input_file to ${output_name}.parquet"
