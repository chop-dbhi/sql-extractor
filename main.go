package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	nats "github.com/nats-io/go-nats"
	"github.com/robfig/cron"
)

func main() {
	args := os.Args[1:]

	if len(args) == 0 {
		log.Fatal("Please specify a config file.")
	}

	config, err := ReadConfig(args[0])
	if err != nil {
		log.Fatal(err)
	}

	queries, err := config.ReadQueries()
	if err != nil {
		log.Fatal(err)
	}

	if len(queries) == 0 {
		log.Print("No queries found.")
		return
	}

	var nc *nats.Conn
	if config.NATS.URL != "" {
		nc, err = nats.Connect(config.NATS.URL)
		if err != nil {
			log.Fatalf("error connecting to nats: %s", err)
		}
	}

	log.Printf("Found %d queries.", len(queries))

	cxt, done := context.WithCancel(context.Background())

	// Handle signals.
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Kill, os.Interrupt)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Separate thread to stop main thread.
	go func() {
		defer wg.Done()

		// Cancel if signal is received.
		select {
		case <-sig:
			done()
		case <-cxt.Done():
		}
	}()

	// No schedule, one-off invocation.
	if config.Schedule.Cron == "" {
		log.Printf("No schedule specified, running one extract")
		if err := Schedule(cxt, nc, config, queries); err != nil {
			log.Print(err)
		}
		done()
	} else {
		log.Printf("Schedule found: %s", config.Schedule.Cron)
		wg.Add(1)

		// Create and schedule job.
		go func() {
			defer wg.Done()

			c := cron.NewWithLocation(time.Local)
			c.AddFunc(config.Schedule.Cron, func() {
				Schedule(cxt, nc, config, queries)
			})
			c.Start()

			// Block
			<-cxt.Done()

			c.Stop()
		}()
	}

	// Wait to cleanup threads.
	wg.Wait()
}
