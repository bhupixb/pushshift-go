package processor

import (
	"fmt"
	"log"
	"time"
)

// Processor interface defines the common method for all strategies
type Processor interface {
	Process(inputPath, outputPath string) (ProcessStats, error)
}

// ProcessStats holds statistics about the processed data
type ProcessStats struct {
	TotalLines    int64
	ExecutionTime time.Duration
}

// String returns a formatted string with process statistics
func (ps ProcessStats) String() string {
	return "📊 Statistics:\n" +
		"  📝 Total lines processed: " + formatCount(ps.TotalLines) + "\n" +
		"  ⏱️  Execution time: " + ps.ExecutionTime.String()
}

// formatCount formats a count with thousands separator
func formatCount(count int64) string {
	return fmt.Sprintf("%d", count)
}

// InitializeLogger sets up the logger with appropriate format
func InitializeLogger() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}
