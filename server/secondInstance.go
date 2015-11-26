package main

import (
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"strconv"
)

type Pair struct {
	Key   int    `json:"key"`
	Value string `json:"value"`
}

type AllPair struct {
	Pairs []Pair `json:"pairs"`
}

var pairMap map[int]Pair

func Put(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	var pair Pair
	key, err := strconv.Atoi(p.ByName("key"))
	if err != nil {
		panic(err)
	}
	value := p.ByName("value")

	pair.Key = key
	pair.Value = value
	pairMap[key] = pair
	result, _ := json.Marshal(pair)

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(201)
	fmt.Fprintf(rw, "%s", result)
}

func Get(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	inputKey := convert(p.ByName("key"))

	var findPair Pair
	for key, value := range pairMap {
		if key == inputKey {
			findPair.Key = inputKey
			findPair.Value = value.Value
		}
	}
	result, _ := json.Marshal(findPair)

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(201)
	fmt.Fprintf(rw, "%s", result)
}

func GetAll(rw http.ResponseWriter, req *http.Request, p httprouter.Params) {
	var pairs []Pair
	for key, value := range pairMap {
		tempPair := Pair{
			key,
			value.Value,
		}
		pairs = append(pairs, tempPair)
	}
	var allPair AllPair
	allPair.Pairs = pairs

	result, _ := json.Marshal(allPair)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(201)
	fmt.Fprintf(rw, "%s", result)
}

func convert(str string) int {
	result, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return result
}

func main() {
	pairMap = make(map[int]Pair)
	firstRouter := httprouter.New()
	firstRouter.PUT("/keys/:key/:value", Put)
	firstRouter.GET("/keys/:key", Get)
	firstRouter.GET("/keys", GetAll)
	firstServer := http.Server{
		Addr:    "0.0.0.0:3001",
		Handler: firstRouter,
	}
	firstServer.ListenAndServe()
}
