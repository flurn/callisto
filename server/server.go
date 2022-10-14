package server

import (
	"context"
	"fmt"
	"github.com/codegangsta/negroni"
	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/kafka"
	producerService "github.com/flurn/callisto/server/service"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func StartAPIServer() {
	server := negroni.New(negroni.NewRecovery())
	//Creates Topic while startup or ignore if already created
	kafka.CreateTopics([]string{"batch_created"}, config.Kafka(), true)

	client := kafka.GetClient()

	service := producerService.NewService(client)

	r := router(service)
	server.UseHandler(r)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf(":%s", strconv.Itoa(config.Port())),
		Handler: server,
	}

	go listenHTTPServer(httpServer)
	waitForShutdown(httpServer)

}

func listenHTTPServer(httpServer *http.Server) {
	err := httpServer.ListenAndServe()
	if err != http.ErrServerClosed {
		log.Fatal("Server exited because of an error")
	}
}

func waitForShutdown(httpServer *http.Server) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig,
		syscall.SIGINT,
		syscall.SIGTERM)
	<-sig
	if httpServer != nil {
		httpServer.Shutdown(context.Background())
	}
}
