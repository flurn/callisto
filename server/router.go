package server

import (
	producerService "github.com/flurn/callisto/server/service"
	"github.com/gorilla/mux"
	"net/http"
)

func router(producer *producerService.Service) http.Handler {
	router := mux.NewRouter()

	router.HandleFunc("/v1/push", producerService.PushMessage(producer)).
		Methods(http.MethodGet)

	return router
}
