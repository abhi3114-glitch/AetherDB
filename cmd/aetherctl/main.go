// Package main is the entry point for the aetherctl CLI tool.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

var (
	serverAddr = flag.String("server", "http://localhost:9090", "AetherDB server address")
)

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		printUsage()
		os.Exit(1)
	}

	command := flag.Arg(0)
	args := flag.Args()[1:]

	var err error
	switch command {
	case "status":
		err = cmdStatus()
	case "query":
		if len(args) < 1 {
			fmt.Println("Usage: aetherctl query <query_string>")
			os.Exit(1)
		}
		err = cmdQuery(strings.Join(args, " "))
	case "write":
		err = cmdWrite()
	case "flush":
		err = cmdFlush()
	case "compact":
		err = cmdCompact()
	case "downsample":
		err = cmdDownsample()
	case "export":
		err = cmdExport(args)
	case "import":
		err = cmdImport()
	case "help":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("AetherDB CLI Tool")
	fmt.Println()
	fmt.Println("Usage: aetherctl [options] <command> [args]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  --server=<addr>  Server address (default: http://localhost:9090)")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  status           Show database status")
	fmt.Println("  query <query>    Execute a query")
	fmt.Println("  write            Write data from stdin (line protocol)")
	fmt.Println("  flush            Force memtable flush")
	fmt.Println("  compact          Trigger compaction")
	fmt.Println("  downsample       Trigger downsampling")
	fmt.Println("  export           Export data")
	fmt.Println("  import           Import data from stdin")
	fmt.Println("  help             Show this help")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  aetherctl status")
	fmt.Println("  aetherctl query 'avg(cpu_usage[5m])'")
	fmt.Println("  echo 'cpu_usage,host=server1 45.2' | aetherctl write")
}

func cmdStatus() error {
	resp, err := http.Get(*serverAddr + "/api/v1/status")
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	data := result["data"].(map[string]interface{})
	memtable := data["memtable"].(map[string]interface{})

	fmt.Println("AetherDB Status")
	fmt.Println("===============")
	fmt.Printf("Series:        %v\n", data["numSeries"])
	fmt.Printf("Blocks:        %v\n", data["numBlocks"])
	fmt.Printf("Total Samples: %v\n", data["totalSamples"])
	fmt.Println()
	fmt.Println("Memtable:")
	fmt.Printf("  Size:     %v bytes\n", memtable["size"])
	fmt.Printf("  Min Time: %v\n", formatTimestamp(memtable["minTime"]))
	fmt.Printf("  Max Time: %v\n", formatTimestamp(memtable["maxTime"]))

	return nil
}

func cmdQuery(queryStr string) error {
	// URL encode the query
	params := url.Values{}
	params.Set("query", queryStr)

	resp, err := http.Get(*serverAddr + "/api/v1/query?" + params.Encode())
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Pretty print JSON
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, body, "", "  "); err != nil {
		fmt.Println(string(body))
		return nil
	}
	fmt.Println(prettyJSON.String())

	return nil
}

func cmdWrite() error {
	// Read from stdin
	scanner := bufio.NewScanner(os.Stdin)
	var lines []string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			lines = append(lines, line)
		}
	}

	if len(lines) == 0 {
		return fmt.Errorf("no data provided")
	}

	body := strings.Join(lines, "\n")
	resp, err := http.Post(
		*serverAddr+"/api/v1/write",
		"text/plain",
		strings.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("write failed: %s", string(respBody))
	}

	fmt.Printf("Wrote %d samples\n", len(lines))
	return nil
}

func cmdFlush() error {
	resp, err := http.Post(*serverAddr+"/api/v1/admin/flush", "", nil)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("flush failed: %s", string(respBody))
	}

	fmt.Println("Flush completed")
	return nil
}

func cmdCompact() error {
	resp, err := http.Post(*serverAddr+"/api/v1/admin/compact", "", nil)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	fmt.Println("Compaction triggered")
	return nil
}

func cmdDownsample() error {
	resp, err := http.Post(*serverAddr+"/api/v1/admin/downsample", "", nil)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("downsample failed: %s", string(respBody))
	}

	fmt.Println("Downsampling completed")
	return nil
}

func cmdExport(args []string) error {
	// Parse export options
	var start, end string
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "--start=") {
			start = strings.TrimPrefix(args[i], "--start=")
		} else if strings.HasPrefix(args[i], "--end=") {
			end = strings.TrimPrefix(args[i], "--end=")
		}
	}

	// Build query
	params := url.Values{}
	if start != "" {
		params.Set("start", start)
	}
	if end != "" {
		params.Set("end", end)
	}

	resp, err := http.Get(*serverAddr + "/api/v1/series?" + params.Encode())
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))

	return nil
}

func cmdImport() error {
	return cmdWrite()
}

func formatTimestamp(ts interface{}) string {
	if ts == nil {
		return "N/A"
	}

	var msec int64
	switch v := ts.(type) {
	case float64:
		msec = int64(v)
	case int64:
		msec = v
	default:
		return "N/A"
	}

	if msec <= 0 || msec == 9223372036854775807 {
		return "N/A"
	}

	t := time.UnixMilli(msec)
	return t.Format(time.RFC3339)
}
