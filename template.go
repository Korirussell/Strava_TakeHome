package main

import (
	"log"

	flag "github.com/spf13/pflag"
)

func main() {
	endpoint := flag.String("endpoint", "", "Logging endpoint")
	debug := flag.Bool("debug", false, "Debug flag used to run locally")
	days := flag.Int("days", 7, "Number of days of data to parse")
	flag.Parse()

	var data []IndexInfo

	if *debug {
		data, err := getDataFromFile("indexes.json")
		if err != nil {
			log.Fatalln("Error reading data from file. Error: ", err.Error())
		}
	} else {
		data, err := getDataFromServer(*endpoint, *days)
		if err != nil {
			log.Fatalln("Error reading data from api endpoint. Error: ", err.Error())
		}
	}
	printLargestIndexes(data)
	printMostShards(data)
	printLeastBalanced(data)
}
