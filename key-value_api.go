package main

import (
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"
)

func main() {
	router := mux.NewRouter()

	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	logger.Info().Msg("Starting server on port 4000")

	router.HandleFunc("/v1/key/{key}", putHandler).Methods(http.MethodPut)

	logger.Fatal().
		Err(http.ListenAndServe(":4000", router)).Msg("Server exited")
}
