package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func helloMuxHandler(w http.ResponseWriter, r *http.Request) {
	_, _ = fmt.Fprintln(w, "Hello gorilla/mux")
}

func main() {
	router := mux.NewRouter()

	router.HandleFunc("/", helloMuxHandler)
	log.Fatal(http.ListenAndServe(":4000", router))
}
