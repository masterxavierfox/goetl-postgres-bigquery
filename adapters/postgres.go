package adapters

import (
	"github.com/masterxavierfox/goetl-postgres-bigquery/config"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"time"
)

//Runs ETL job decider for daemon or non-daemon mode
func EtlJobDecider(daemon bool,table,columnSelect, startDate,endDate string) {

	switch daemon{
	case true:
		pgEtlDaemonJob()
	case false:
		pgEtlManualJob(table,columnSelect,startDate,endDate)
	default:
		log.Fatalf("ERROR: Please check your configs.")

	}

}

//Runs ETL job for non-daemon mode
func pgEtlManualJob (table,columnSelect, startDate,endDate string){
	config.CheckCmdExists("psql")
	config.LoadEnv()

	//Pg options
	tblExport := "'"+table+"-"+startDate+".csv'"
	dayStart := "'"+startDate+"'"
	dayEnd := "'"+endDate+"'"

	//Cleanup workspace
	DeleteArchive(""+table+"-"+startDate+".csv.gz")

	log.Info("MANUAL WORKFLOW STARTED :::::: Downloading table: "+ table +":" + startDate + "...")
	cmd := exec.Command("psql","-c", "\\copy (select "+columnSelect+" from "+table+" where created_at >= "+dayStart+" and created_at < "+dayEnd+") TO "+tblExport+" WITH CSV HEADER")
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Error("PSQL COMMAND: failed with %s\n", err)
	}
	log.Info("PSQL STATUS: ", string(out))

	//gZIP DATABASE
	tblExportArc := ""+table+"-"+startDate+".csv"
	log.Info(":::: ========== CREATING ARCHIVE ========= ::::")
	config.CmdEngine("gzip",tblExportArc)

	//UPLOAD TO BIG QUERY
	err = bQUpload(config.BQ_DATASET_ID, config.BQ_TABLE_ID, tblExportArc+".gz")
	if err != nil {
		log.Fatal(err)
	}


}

//Runs ETL job for daemon mode
func pgEtlDaemonJob (){
	config.LoadEnv()
	var tables = []string{os.Getenv("PGTABLES")}

	//for i := 0; i < len(tables); i++ {
	for i := range tables {
		//Get start sync date
		syncStartDate := pgdaemonDatesearch("MIN",tables[i],"created_at","START")

		//Get start sync date
		syncEndDate := pgdaemonDatesearch("MAX",tables[i],"created_at","END")


		//Get Last job time
		//syncTime := config.RedKeep(tables[i],syncEndDate)
		startTime := time.Now().String()


		//Parse date to check if its correct
		log.Info("| PG :: START-SYNC-DATE | record: ",syncStartDate)
		log.Info("| PG :: END-SYNC-DATE | record: ",syncEndDate)
		//time, err := time.Parse(time.RFC3339,syncTime);
		//if err != nil {
		//	log.Fatal("| DATE-FORMAT-ERROR | we have: ",time)
		//}else {
			log.Info("| PG ::: JOB START TIME: "+ startTime[0:19] +"| SYNC-TABLE |: "+tables[i] )//+" | REDIS: "+config.RedKeepPing())
			//Dump and pump
			pgUploadday(syncStartDate,syncEndDate,tables[i],startTime)
	//	}


		////Check if database has been synced or not
		//switch state{
		//
		//case "create":
		//	log.Info("| NEW-SYNC-DATE | record: ",syncTime)
		//	time, err := time.Parse(time.RFC3339,syncTime);
		//	if err != nil {
		//		log.Fatal("| DATE-FORMAT-ERROR | we have: ",time)
		//	}else{
		//		log.Info("| SYNC-DATE |: "+ syncTime +"REDIS |:"+config.RedKeepPing())
		//		//Dump and pump
		//		pgUploadday(syncStartDate,syncEndDate,tables[i])
		//		return syncStartDate
		//	}
		//case "update":
		//	//Get start sync date
		//	upSyncStartDate := pgdaemonDatesearch("MIN",tables[i],"updated_at")
		//
		//	//Get start sync date
		//	upSyncEndDate := pgdaemonDatesearch("MAX",tables[i],"updated_at")
		//
		//
		//	upSyncTime ,state:= config.RedkeepUpdate(tables[i],upSyncEndDate)
		//
		//	log.Info("| SYNC-DATE |: Bypassing Redis. | REDIS |:"+config.RedKeepPing())
		//
		//	if syncTime != "" {
		//		log.Info("| SYNC-DATE | record: ",syncTime)
		//		//Dump and pump
		//		pgUploadday(syncStartDate,syncEndDate,tables[i])
		//		return syncTime
		//	}else{
		//		log.Info("| SYNC-DATE |: Bypassing Redis. | REDIS |:"+config.RedKeepPing())
		//		//Dump and pump
		//		pgUploadday(syncEndDate,upSyncEndDate,tables[i])
		//		return syncStartDate
		//	}
		//
		//
		//default:
		//	log.Info("All Synced up baby")
		//
		//
		//}


	}

}

//initiate big query upload and postgres db dump.
func pgUploadday (syncStart,syncEnd,table,jobtime string){
	config.LoadEnv()
	tblExport := table+"-"+syncStart[0:10]+".csv"

	firstDate :=syncStart[0:10]
	secondDate := syncEnd[0:10]
	dbjobTime := table+"-"+jobtime[0:10]+".csv"


	//Cleanup workspace
	//DeleteArchive(tblExport+".gz")

		//Dump database to CSV
		log.Info("DAEMON WORKFLOW STARTED ::::::\n Downloading table: "+ table +".\n From - " + syncStart[0:10] + ".\n To - "+syncEnd[0:10]+".\n Job Time: "+jobtime)

		//cmd := exec.Command("psql", "-c", "\\copy (select "+config.COLUMN_SELECT+" from "+table+" where "+column+" >= "+syncStart[0:10]+" and "+column+" <= "+syncEnd[0:10]+") TO "+table+"-"+syncStart[0:10]+".csv WITH CSV HEADER")
	    cmd := exec.Command("psql","-c", "\\copy (select "+config.COLUMN_SELECT+" from "+table+" where created_at >= '"+firstDate+"' and created_at <= '"+secondDate+"') TO "+dbjobTime+" WITH CSV HEADER")
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Errorf("ERROR:\n%s\n", string(out))
			log.Fatal("PSQL: failed with %s\n", err,cmd)

		}


		//gZIP DATABASE
		config.CmdEngine("gzip",tblExport)

		//UPLOAD TO BIG QUERY
		//err = bQUpload(config.BQ_DATASET_ID, config.DB_NAME+"_"+table, ""+table+"-"+syncStart+".csv.gz")
		//if err != nil {
		//	log.Fatal(err)
		//}

		//Delete archive
		config.CmdEngine("rm",tblExport+".gz")


}

//Search database for the max or min date
func pgdaemonDatesearch (c,table string,column string,stage string) string{


	//Get date record from database

	cmd := exec.Command("psql", "-qAt","-c", "select "+c+"("+column+") from public."+table)
	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Errorf("PSQL: failed with %s\n", err)
	}
	log.Info(stage+" : RECORD DATE: ", string(out[0:19]))
	date := string(out[0:19])


	return date
}

//Delete database archive
func DeleteArchive (tblExportArchive string){
	//Delete archive
	log.Info(":::: ========== DELETING ARCHIVE ========= ::::")
	config.CmdEngine("rm",tblExportArchive)
	//config.NewCommander("rm",tblExportArchive)
}
