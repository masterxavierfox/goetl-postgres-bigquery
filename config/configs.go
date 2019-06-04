package config

import (
	"fmt"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"time"
	"github.com/go-redis/redis"
)


//ASCII Character generator
// http://patorjk.com/software/taag/#p=display&f=ANSI%20Shadow&t=PG2BQ
const PG2BQ_ASCII_LOGO = `
█▀▀█ █▀▀▀ ░ ░ █▀█ ░ ░ █▀▀▄ █▀▀█
█░░█ █░▀█ ▀ ▀ ░▄▀ ▀ ▀ █▀▀▄ █░░█
█▀▀▀ ▀▀▀▀ ░ ░ █▄▄ ░ ░ ▀▀▀░ ▀▀▀█
`


var (
    //BIG QUERY CONNECTION SPECIFICS
	GOOGLE_APPLICATION_CREDENTIALS string = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	BQ_PROJECT_ID string = os.Getenv("BQ_PROJECT_ID")
	BQ_DATASET_ID string = os.Getenv("BQ_DATASET_ID")
	BQ_TABLE_ID string = os.Getenv("BQ_TABLE_ID")

	//PG DATABASE CONNECTION SPECIFICS
	DB_HOST string = os.Getenv("PGHOST")
	DB_PORT string = os.Getenv("PGPORT")
	DB_USER string = os.Getenv("PGUSER")
	DB_PASS string = os.Getenv("PGPASSWORD")
	DB_NAME string = os.Getenv("PGDATABASE")

	//PG DATABASE EXPORT SPECIFICS
	COLUMN_SELECT string = os.Getenv("PGCOLUMNS")
	START_DAY string = os.Getenv("START_DAY")
	END_DAY string = os.Getenv("END_DAY")

	//REDIS DATABASE
	REDIS_ADDR string = os.Getenv("REDIS_ADDR")
	REDIS_PASS string = os.Getenv("REDIS_PASS")
	REDIS_PREFIX string = os.Getenv("REDIS_PREFIX")
)

//Load up environment variables
func LoadEnv(){
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
		os.Exit(1)
	}
	//BIG QUERY CONNECTION SPECIFICS
	GOOGLE_APPLICATION_CREDENTIALS  = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	BQ_PROJECT_ID  = os.Getenv("BQ_PROJECT_ID")
	BQ_DATASET_ID  = os.Getenv("BQ_DATASET_ID")
	BQ_TABLE_ID  = os.Getenv("BQ_TABLE_ID")

	//PG DATABASE CONNECTION SPECIFICS
	DB_HOST  = os.Getenv("PGHOST")
	DB_PORT  = os.Getenv("PGPORT")
	DB_USER  = os.Getenv("PGUSER")
	DB_PASS  = os.Getenv("PGPASSWORD")
	DB_NAME  = os.Getenv("PGDATABASE")

	//PG DATABASE EXPORT SPECIFICS
	COLUMN_SELECT = os.Getenv("PGCOLUMNS")
	START_DAY  = os.Getenv("START_DAY")
	END_DAY  = os.Getenv("END_DAY")

	//REDIS DATABASE
	REDIS_ADDR  = os.Getenv("REDIS_ADDR")
	REDIS_PASS  = os.Getenv("REDIS_PASS")
	REDIS_PREFIX  = os.Getenv("REDIS_PREFIX")
}

//Verify executable command exists
func CheckCmdExists(cmd string) {
	path, err := exec.LookPath(cmd)
	if err != nil {
		log.Printf("::: OOPS ::: we didn't find "+cmd+" executable, kindly check.\n")
	} else {
		log.Printf("::: INIT OK ::: '%s'\n", path)
	}
}

//Generic command engine with output.
func CmdEngine (c string,args string) {
	LoadEnv()
	log.Info("Executing...: "+ c +".")
	cmd := exec.Command(c, args)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Errorf(c+" status :\n%s\n", string(out))
		log.Fatalf("ERROR: "+c+" "+args+" failed with \n", err)
	}
}
//
//func NewCommander (c string,args string,args2 string){
//	// Disable output buffering, enable streaming
//	cmdOptions := cmd.Options{
//		Buffered:  false,
//		Streaming: true,
//	}
//
//	// Create Cmd with options
//	envCmd := cmd.NewCmdOptions(cmdOptions, c,args,args2)
//
//	// Print STDOUT and STDERR lines streaming from Cmd
//	go func() {
//		for {
//			select {
//			case line := <-envCmd.Stdout:
//				log.Info(line)
//			case line := <-envCmd.Stderr:
//				log.Info(os.Stderr, line)
//			}
//		}
//	}()
//
//	// Run and wait for Cmd to return, discard Status
//	<-envCmd.Start()
//
//	// Cmd has finished but wait for goroutine to print all lines
//	for len(envCmd.Stdout) > 0 || len(envCmd.Stderr) > 0 {
//		time.Sleep(10 * time.Millisecond)
//	}
//}
//
//
//

func RedKeep(tableName string, lastSync string) (string){
	Rclient := redis.NewClient(&redis.Options{
		Addr:               REDIS_ADDR,
		Password:           REDIS_PASS,
		DB:                 0, // use default DB
	})

	redkey := REDIS_PREFIX+tableName
	createState := "create"
	syncBookmark := REDIS_PREFIX+tableName+"sync"
	time := time.Now().String()
	syncJobTime := time[0:19]



	bookTime, err := Rclient.Get(syncBookmark).Result()
	switch{

	case err == redis.Nil:
		log.Info("| REDKEEP BOOKMARK |: 404 - Bookmark "+syncBookmark+" not done..."+bookTime)
		//Search and save
		syncTime, err := Rclient.Get(redkey+"-"+createState).Result()
		if err == redis.Nil {
			log.Info("| REDKEEP BOOKMARK |: Table "+redkey+"-"+createState+" time sync does not exist..saving")
			//Save Table record with state
			Rerr := Rclient.Set(redkey+"-"+createState, lastSync, 0).Err()
			if Rerr != nil {
				PanicAndRecover("Chale We Hot! we could not save the track to redis.")
				log.Fatal(Rerr)
				return "nil"
			}
			savedTabletime, _:= Rclient.Get(redkey+"-"+createState).Result()
			_= Rclient.Set(syncBookmark, syncJobTime, 0).Err()
			return savedTabletime
		} else if err != nil {
			log.Fatal("| REDKEEP |:Something Went wrong - Check Redis.|\n "+REDIS_ADDR+" |\n "+REDIS_PASS+" |\n ",err)
			return "nil"
		} else {
			jobTime,_ := Rclient.Get(syncBookmark).Result()
			//redkeepUpdate(syncBookmark,syncJobTime[0:10])
			log.Info("| REDKEEP BOOKMARK |: Time sync bookmark Found | ID: "+ syncBookmark +"| JOB TIME: "+syncJobTime +"::::|Last Record TIME: "+ syncTime)
			return jobTime
		}
	case err != nil:
		log.Fatal("| REDKEEP |:Something Went wrong - Check Redis.|\n "+REDIS_ADDR+" |\n "+REDIS_PASS+" |\n ",err)
		return "nil"

	default:
		jobTime,err:= Rclient.Get(syncBookmark).Result()
		if err != nil {
			PanicAndRecover("Chale We Hot! we could not save the track to redis.")
			log.Fatal(err)
			return "nil"
		}
		log.Info("| REDKEEP UPDATE|: Time sync bookmark Found | ID: "+ syncBookmark +"| JOB TIME: "+fmt.Sprint(syncJobTime) )
		redkeepUpdate(syncBookmark,syncJobTime)
		return jobTime
	}
}


func redkeepUpdate (key string,value string){
	Rclient := redis.NewClient(&redis.Options{
		Addr:               REDIS_ADDR,
		Password:           REDIS_PASS,
		DB:                 0, // use default DB
	})

	log.Info("| REDKEEP UPDATE| KEY: "+ key +" | JOB TIME - "+value)
	//Save Table record with state
	Rerr := Rclient.Set(key, value, 0).Err()
	if Rerr != nil {
		PanicAndRecover("Chale We Hot! we could not save the track to redis.")
		log.Fatal(Rerr)
		//return "nil"
	}
	log.Info("| REDKEEP UPDATE | JOB  TIME UPDATED TO :"+ value)
	//savedTable, _:= Rclient.Get(redkey+"-"+updateState).Result()
	//return savedTable,updateState


}


func RedKeepPing() string{
	Rclient := redis.NewClient(&redis.Options{
		Addr:               REDIS_ADDR,
		Password:           REDIS_PASS,
		DB:                 0, // use default DB
	})

	pong, err := Rclient.Ping().Result()
	if err != nil{
		log.Warn("| REDKEEP-PING |: We cannot reach the RedKeep, check your redis connection.")
	} else {
		return pong
	}
	return ""

}

func PanicAndRecover(message string) string{
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	panic(message)
}
