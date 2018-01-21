package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/chop-dbhi/sql-extractor"
	"github.com/chop-dbhi/sql-extractor/events"
	nats "github.com/nats-io/go-nats"
	"github.com/robfig/cron"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	args := os.Args[1:]
	if len(args) == 0 {
		return errors.New("Please specify a config file.")
	}

	// Load config.
	config, err := sqlextractor.ReadConfig(args[0])
	if err != nil {
		return err
	}

	// Read queries.
	queries, err := config.ReadQueries()
	if err != nil {
		return err
	}

	// Nothing to do.
	if len(queries) == 0 {
		log.Print("No queries found.")
		return nil
	}

	log.Printf("Found %d queries.", len(queries))

	// Connect to NATS.
	nc, err := nats.Connect(config.NATS.URL)
	if err != nil {
		return err
	}
	defer nc.Close()

	pub := &events.Publisher{
		Conn:   nc,
		Logger: log.New(os.Stderr, "", 0),
	}

	ctx, done := context.WithCancel(context.Background())
	defer done()

	// No schedule, single execution.
	if config.Schedule.Cron == "" {
		log.Printf("No schedule specified, executing once.")
		return sqlextractor.Execute(ctx, pub, config, queries)
	}

	log.Printf("Schedule found: %s", config.Schedule.Cron)

	// Start cron-based scheduler.
	c := cron.NewWithLocation(time.Local)
	c.AddFunc(config.Schedule.Cron, func() {
		if err := sqlextractor.Execute(ctx, pub, config, queries); err != nil {
			log.Print(err)
		}
	})
	c.Start()
	defer c.Stop()

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Kill, os.Interrupt)
	<-sig

	return nil
}
