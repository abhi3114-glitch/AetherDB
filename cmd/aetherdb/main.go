// Package main is the entry point for AetherDB server.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"aetherdb/internal/api"
	"aetherdb/internal/downsample"
	"aetherdb/internal/storage"
)

func main() {
	var (
		dataDir = flag.String("data-dir", "./data", "Data directory for storage")
		port    = flag.Int("port", 9090, "HTTP server port")
		help    = flag.Bool("help", false, "Show help")
	)
	flag.Parse()

	if *help {
		printHelp()
		os.Exit(0)
	}

	// Print banner
	printBanner()

	log.Printf("Starting AetherDB...")
	log.Printf("Data directory: %s", *dataDir)
	log.Printf("Port: %d", *port)

	// Create storage engine
	engine, err := storage.NewEngine(storage.EngineConfig{
		DataDir: *dataDir,
	})
	if err != nil {
		log.Fatalf("Failed to create storage engine: %v", err)
	}

	// Create downsampling scheduler
	scheduler := downsample.NewScheduler(engine, *dataDir)
	scheduler.Start()

	// Create API server
	addr := fmt.Sprintf(":%d", *port)
	server := api.NewServer(engine, scheduler, addr)

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down...")

		scheduler.Stop()
		if err := engine.Close(); err != nil {
			log.Printf("Error closing engine: %v", err)
		}

		os.Exit(0)
	}()

	// Start server
	if err := server.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func printBanner() {
	banner := `
    _         _   _               ____  ____  
   / \   ___| |_| |__   ___ _ __|  _ \| __ ) 
  / _ \ / _ \ __| '_ \ / _ \ '__| | | |  _ \ 
 / ___ \  __/ |_| | | |  __/ |  | |_| | |_) |
/_/   \_\___|\__|_| |_|\___|_|  |____/|____/ 
                                              
Time-Series Database v1.0.0
`
	fmt.Println(banner)
}

func printHelp() {
	fmt.Println("AetherDB - Time-Series Database")
	fmt.Println()
	fmt.Println("Usage: aetherdb [options]")
	fmt.Println()
	fmt.Println("Options:")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  aetherdb --data-dir=/var/lib/aetherdb --port=9090")
	fmt.Println()
	fmt.Println("API Endpoints:")
	fmt.Println("  POST /api/v1/write         Write metrics (line protocol)")
	fmt.Println("  GET  /api/v1/query         Execute instant query")
	fmt.Println("  GET  /api/v1/query_range   Execute range query")
	fmt.Println("  GET  /api/v1/series        List all series")
	fmt.Println("  GET  /api/v1/status        Get database status")
	fmt.Println("  GET  /health               Health check")
}
