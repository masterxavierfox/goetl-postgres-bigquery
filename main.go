package main

import (
	"flag"
	"fmt"
	"github.com/masterxavierfox/goetl-postgres-bigquery/adapters"
	"github.com/masterxavierfox/goetl-postgres-bigquery/config"
	"os"
	log "github.com/sirupsen/logrus"

)

func main()  {
	//Load up environment variables

	config.LoadEnv()
	//Get parameters from cmd input

	var (
		tableName string
		columnSelection string
		startDate string
		endDate string
		daemon bool
	)

	//Runs flags for non-daemon mode
	flag.StringVar(&tableName, "table", os.Getenv("TABLE_NAME"), "Usage: enter database table name to use e.g: -table billing.")
	flag.StringVar(&columnSelection, "column", "*", "Usage: enter columns to select. e.g: -column * or -column created_at")
	flag.BoolVar(&daemon,"daemon",false,"Usage: run as daemon. e.g: -daemon true ")

	//Runs flags for daemon mode
	flag.StringVar(&startDate, "start", os.Getenv("START_EXPORT_DATE"), "Usage: enter export start date. e.g: -start 2019-01-01 ")
	flag.StringVar(&endDate, "end", os.Getenv("END_EXPORT_DATE"), "Usage: enter export end date. e.g: -end 2019-01-01 ")



	//Parse the flags
	flag.Parse()

	//Check the flags
	switch{
	case daemon == true && startDate != "" && endDate != "" && tableName != "":
		log.Println("::: DAEMON JOB ::: ==================================== STARTING RAD PG-TO-BIGQUERY ETL COMMANDER ========================================")
		adapters.EtlJobDecider(daemon,tableName,columnSelection,startDate,endDate)
	case daemon != true && tableName != "" && columnSelection != "" && startDate != "":
		log.Info("::: MANUAL JOB ::: ==================================== STARTING RAD PG-TO-BIGQUERY ETL COMMANDER ========================================: \n -::Daemon | ",daemon,"\n -::Table Name | ",tableName,"\n -::Columns Selected | ",columnSelection,"\n -::Date Start | ",startDate,"\n -::Date End | ",endDate,"\n -::Trailing | ",flag.Args())
		adapters.EtlJobDecider(daemon,tableName,columnSelection,startDate,endDate)
	default:
		fmt.Println(config.PG2BQ_ASCII_LOGO)
		fmt.Println("=============== Eish! mbaba!, Please give us some flags see below: ===============")
		flag.PrintDefaults()
		os.Exit(1)

	}
}
