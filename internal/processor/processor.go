package processor

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	partSizeThreshold = 8 * 1024 * 1024 * 1024 // 8GB in bytes for each part file
	bufferSize        = 512 * 1024 * 1024      // 512MB buffer for reading
	scannerBufferSize = 512 * 1024 * 1024      // 512MB buffer for scanner
)

// PushshiftProcessor represents the processor for processing Pushshift data
// Process flow: Decompress file -> write to part files of 8GB -> convert each part to parquet using DuckDB
type PushshiftProcessor struct{}

// Process implements the processor interface
// It decompresses the input zst file, splits it into parts, and converts each part to Parquet format
func (s *PushshiftProcessor) Process(inputPath, outputPath string) (ProcessStats, error) {
	start := time.Now()
	stats := ProcessStats{}

	log.Printf("📖 Reading and processing zst file: %s", inputPath)

	// Open input file
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return stats, fmt.Errorf("failed to open input file: %v", err)
	}
	defer inputFile.Close()

	// Create zstd reader
	zr, err := zstd.NewReader(inputFile)
	if err != nil {
		return stats, fmt.Errorf("failed to create zstd reader: %v", err)
	}
	defer zr.Close()

	// Create a buffered reader around the decompressor for better performance
	bufferedReader := bufio.NewReaderSize(zr, bufferSize)

	partNum := 1
	totalBytesProcessed := int64(0)
	startTime := time.Now()
	var lastPartWritten bool

	// Create scanner for reading line by line
	scanner := bufio.NewScanner(bufferedReader)
	// Set a larger buffer for scanner to handle potentially large JSON lines
	scanBuf := make([]byte, scannerBufferSize)
	scanner.Buffer(scanBuf, scannerBufferSize)

	for {
		// Process one part file
		partPath := fmt.Sprintf("%s_part_%03d.jsonl", outputPath, partNum)
		bytesWritten, linesProcessed, err := processPartFile(scanner, partPath)

		// Only consider this a successful write if we wrote some data
		if bytesWritten > 0 {
			lastPartWritten = true
			totalBytesProcessed += bytesWritten
			stats.TotalLines += linesProcessed

			// Log progress
			elapsed := time.Since(startTime)
			speed := float64(totalBytesProcessed) / elapsed.Seconds() / 1024 / 1024 // MB/s
			log.Printf("📊 Part %d: Processed %d lines, %.2f MB/s, %.2f MB written",
				partNum, linesProcessed, speed, float64(bytesWritten)/1024/1024)

			// Convert to Parquet using DuckDB
			log.Printf("🔄 Converting part %d to Parquet format...", partNum)
			parquetBaseName := fmt.Sprintf("%s_part_%03d", outputPath, partNum)
			err = convertToParquet(partPath, parquetBaseName)
			if err != nil {
				return stats, fmt.Errorf("failed to convert part %d to parquet: %v", partNum, err)
			}

			// Remove the JSONL file after successful conversion
			if err := os.Remove(partPath); err != nil {
				log.Printf("⚠️ Warning: Failed to remove intermediate file %s: %v", partPath, err)
			}

			partNum++
		} else if !lastPartWritten {
			// If we didn't write anything and never wrote a part before, return an error
			return stats, fmt.Errorf("no data was written from the input file")
		}

		// Handle errors or EOF
		if err != nil {
			if err == io.EOF {
				log.Printf("✅ Reached end of input file")
				break
			}
			return stats, fmt.Errorf("failed to process part %d: %v", partNum, err)
		}
	}

	// Calculate final stats
	stats.ExecutionTime = time.Since(start)
	log.Printf("✅ Processing complete")
	log.Printf("%s", stats.String())

	return stats, nil
}

// processPartFile processes one part file until it reaches the size threshold
func processPartFile(scanner *bufio.Scanner, outputPath string) (int64, int64, error) {
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return 0, 0, err
	}
	defer outputFile.Close()

	writer := bufio.NewWriterSize(outputFile, bufferSize)
	defer writer.Flush()

	var bytesWritten int64
	var linesProcessed int64

	for bytesWritten < partSizeThreshold {
		if !scanner.Scan() {
			// Check for errors
			if err := scanner.Err(); err != nil {
				return bytesWritten, linesProcessed, fmt.Errorf("scanner error: %v", err)
			}
			// No error means we've reached EOF
			return bytesWritten, linesProcessed, io.EOF
		}

		// Get the line and add newline
		line := scanner.Bytes()

		// Write the line with a newline character
		written, err := writer.Write(line)
		if err != nil {
			return bytesWritten, linesProcessed, fmt.Errorf("error writing line: %v", err)
		}

		// Add newline after each line
		if _, err := writer.Write([]byte("\n")); err != nil {
			return bytesWritten, linesProcessed, fmt.Errorf("error writing newline: %v", err)
		}

		bytesWritten += int64(written + 1) // +1 for newline
		linesProcessed++

		// Log progress occasionally
		if linesProcessed%1000000 == 0 {
			log.Printf("🔄 Progress: Processed %d lines, %.2f MB written",
				linesProcessed, float64(bytesWritten)/1024/1024)
		}
	}

	// Make sure to flush before returning
	if err := writer.Flush(); err != nil {
		return bytesWritten, linesProcessed, fmt.Errorf("error flushing buffer: %v", err)
	}

	return bytesWritten, linesProcessed, nil
}

// convertToParquet converts a JSONL file to Parquet format using DuckDB
func convertToParquet(jsonlPath, outputBaseName string) error {
	// Use absolute path for the script - assuming it's in the project root
	workingDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("failed to get current working directory: %v", err)
	}

	scriptPath := filepath.Join(workingDir, "json_to_parquet_duckdb.sh")

	// Verify the script exists
	if _, err := os.Stat(scriptPath); os.IsNotExist(err) {
		return fmt.Errorf("converter script not found at %s", scriptPath)
	}

	log.Printf("🔧 Using converter script: %s", scriptPath)
	log.Printf("🔧 Converting %s to %s.parquet", jsonlPath, outputBaseName)

	// Run the converter script
	cmd := exec.Command("bash", scriptPath, jsonlPath, outputBaseName)

	// Capture both stdout and stderr
	output, err := cmd.CombinedOutput()
	outputStr := string(output)

	// Log the output regardless of error
	log.Printf("🔄 DuckDB output: %s", outputStr)

	if err != nil {
		return fmt.Errorf("DuckDB conversion failed: %v\nOutput: %s", err, outputStr)
	}

	// Verify the parquet file was created
	parquetPath := outputBaseName + ".parquet"
	if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
		return fmt.Errorf("parquet file was not created at %s", parquetPath)
	}

	log.Printf("✅ Successfully converted %s to %s", filepath.Base(jsonlPath), parquetPath)
	return nil
}
