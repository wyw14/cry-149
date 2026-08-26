package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wyw14/cry-149/internal/api"
	"github.com/wyw14/cry-149/internal/service"
)

func main() {
	address := flag.String("addr", "127.0.0.1:21249", "")
	dataDir := flag.String("data", "./var/fermaloop", "")
	flag.Parse()

	runtime, err := service.New(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{
		Addr: *address, Handler: api.New(runtime).Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	fmt.Printf("FermaLoop listening on %s\n", *address)
	err = server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		log.Printf("server failed: %v", err)
	}
	if closeErr := runtime.Close(); closeErr != nil {
		log.Printf("runtime close failed: %v", closeErr)
	}
}
