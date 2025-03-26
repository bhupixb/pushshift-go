# Pushshift-Go2

Efficiently read large zst files of reddit data from pushshift and convert into Parquet files using DuckDB.

A Go tool for processing large zst compressed JSON files from Pushshift, splitting them into manageable parts, and converting to Parquet format.

## Overview

This tool efficiently processes large zst-compressed JSON files from Pushshift by:

1. Decompressing the zst file on-the-fly.
2. Write the data to a file in JSON format.
3. Once the json file reaches manageable parts (8GB by default), convert it to Parquet format using DuckDB.
4. The magic is done by DuckDB, it reads the json files,
automatically infers the schema & create a DuckDB table. Then we copy that DuckDB table to an output file in Parquet Format. Refer json_to_parquet_duckdb.sh script.

This approach provides:
- Memory-efficient processing of zst files that are too large for single-pass conversion. If we decompress a 50gb zst file to JSON, then it will require us > 1000 GB of storage because the compression ratio of zst:json is 1:~25.
- So instead we are reading the data in chunk of 8gb, covert it to parquet format. The ratio of zst:parquet is 1:~3x only.
- Optimized disk usage by removing intermediate files.

## Prerequisites

- Go 1.19+
- DuckDB installed and available in PATH
- `json_to_parquet_duckdb.sh` script in the project root (used for JSONL to Parquet conversion)

### Installing DuckDB

macOS:
```bash
brew install duckdb
```

Linux:
```bash
# Check your distribution's package manager or download from:
# https://duckdb.org/docs/installation/
```

## Build

```bash
go build -o pushshift-processor ./cmd/processor
```

## Usage

```bash
./pushshift-processor -input=your_data.zst -output=output_prefix
```

The tool will:
1. Process the zst file in chunks
2. Create output files with the pattern: `output_prefix_part_001.parquet`, `output_prefix_part_002.parquet`, etc.

### Command-line parameters

- `-input`: Path to the input zst file (required)
- `-output`: Output file prefix (defaults to "output")

## Converter Script

The project includes a converter script `json_to_parquet_duckdb.sh` in the project root. This script is used to convert JSONL files to Parquet format using DuckDB.

The script takes the following parameters:
```bash
./json_to_parquet_duckdb.sh <jsonl_file> [output_name]
```

### How it works

When you run the processor, it automatically calls this script to convert each part file from JSONL to Parquet format. The script:

1. Takes a zst file as input, decompress it in json.
2. Uses DuckDB to read the JSON data
3. Exports the data to Parquet format
4. Cleans up temporary tables

## Performance Tuning

The processor uses the following buffer sizes, which can be adjusted in the code for different performance characteristics:

```go
const (
    partSizeThreshold = 8 * 1024 * 1024 * 1024 // 8GB for each part file
    bufferSize        = 512 * 1024 * 1024      // 512MB buffer for reading
    scannerBufferSize = 512 * 1024 * 1024      // 512MB buffer for scanner
)
```

- Increase `partSizeThreshold` for fewer, larger output files
- Adjust buffer sizes based on available memory

## Parquet Benefits

The Parquet output format provides several advantages:
- **Column-based storage**: More efficient for analytical queries
- **Built-in compression**: Significantly reduces file size
- **Predicate pushdown**: Query optimization for faster analytics
- **Direct integration**: With tools like DuckDB, Apache Spark, Dask, etc.

## Example Output

On my local Mac M3 12 core, 18gb.
It takes about 2min 21s to decompress a zst file of
size 1.7GB which is ~46GB in uncompressed format(json) and ~3GB in parquet format. 
```
2025/03/26 20:53:07.159027 📊 Statistics:
  📝 Total lines processed: 16680905
  ⏱️  Execution time: 2m21.155298333s

📊 Statistics:
  📝 Total lines processed: 16680905
  ⏱️  Execution time: 2m21.155298333s
2025/03/26 20:53:07.159042 ✅ All done!
```

## Testing the Setup

To verify your setup is working correctly:

1. Create a small test JSONL file with some valid JSON data:
   ```bash
   echo '{"id": 1, "text": "test1"}' > test.jsonl
   echo '{"id": 2, "text": "test2"}' >> test.jsonl
   ```

2. Compress it with zstd:
   ```bash
   # Install zstd if needed
   # macOS: brew install zstd
   # Ubuntu/Debian: apt-get install zstd
   zstd test.jsonl -o test.jsonl.zst
   ```

3. Run the processor:
   ```bash
   ./pushshift-processor -input=test.jsonl.zst -output=test_output
   ```

4. Check the output:
   ```bash
   # Verify the Parquet file was created
   ls -la test_output_part_001.parquet
   
   # You can view the Parquet file contents with DuckDB
   duckdb -c "SELECT * FROM read_parquet('test_output_part_001.parquet');"
   ```

## Troubleshooting

1. **DuckDB not found**: Ensure DuckDB is installed and available in your PATH
2. **Converter script errors**: Make sure the script is executable and has the correct path
3. **Go build errors**: Verify your Go installation and that all dependencies are installed

For any issues, check the error logs which will be displayed when running the processor.
Feel free to open an issue.

## Querying Parquet Files with DuckDB

After processing your data, you can analyze the resulting Parquet files using DuckDB, which provides excellent performance for analytical queries.

### Basic Querying

To run basic queries on your output Parquet files(for e.g. to see the schema etc):

```bash
# Start DuckDB CLI
duckdb

# Create a table
> CREATE TABLE my_table AS
  SELECT * FROM read_parquet('output_file_part001.parquet');

# see all columns and data type
> SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'my_table';

# copy the schema to a file
> copy (SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'my_table') to 'schema.csv' (FORMAT CSV, HEADER TRUE);
```

