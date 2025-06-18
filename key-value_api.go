package main

import (
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
)

func main() {
	router := mux.NewRouter()

	zlog := zerolog.New(os.Stdout).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	err := initializeTransactionLog()
	if err != nil {
		zlog.Fatal().Err(err).Msg("could not initialize Transaction Log")
		return
	}
	zlog.Info().Msg("Starting server on port 4000")

	routes(router)

	zlog.Fatal().
		Err(http.ListenAndServe(":4000", router)).Msg("Server exited")
}

func routes(router *mux.Router) {
	router.HandleFunc("/v1/key/{key}", putHandler).Methods(http.MethodPut)
	router.HandleFunc("/v1/key/{key}", getHandler).Methods(http.MethodGet)

	router.HandleFunc("/v1/key/{key}", deleteHandler).Methods(http.MethodDelete)
}
