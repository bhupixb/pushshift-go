package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/bhupendray/pushshift-go2/internal/processor"
)

func main() {
	// Initialize logger
	processor.InitializeLogger()

	// Define command-line flags
	inputFlag := flag.String("input", "", "Path to input .zst file")
	outputFlag := flag.String("output", "output", "Prefix for output files")

	flag.Parse()

	// Validate command line arguments
	if *inputFlag == "" {
		log.Fatal("❌ Input file path is required. Use -input flag")
	}

	// Check if input file exists
	if _, err := os.Stat(*inputFlag); os.IsNotExist(err) {
		log.Fatal("❌ Input file does not exist:", *inputFlag)
	}

	// Initialize processor
	proc := &processor.PushshiftProcessor{}
	strategyName := "Pushshift Processor (split into parts and convert to Parquet)"

	log.Printf("🚀 Starting %s", strategyName)
	log.Printf("📖 Input file: %s", *inputFlag)
	log.Printf("📝 Output prefix: %s", *outputFlag)

	// Process the file
	stats, err := proc.Process(*inputFlag, *outputFlag)
	if err != nil {
		log.Fatal("❌ Processing failed:", err)
	}

	// Print final stats
	fmt.Println("\n" + stats.String())

	log.Printf("✅ All done!")
}
