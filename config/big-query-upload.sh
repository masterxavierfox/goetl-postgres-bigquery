#!/bin/bash
#https://medium.com/radio-africa-techblog/loading-terabytes-of-data-from-postgres-into-bigquery-ec4ba8978cc9

function upload_day {
  table=$1
  sel=$2
  day=$3
  next_day=$(date -d "$day+1 days" +%Y-%m-%d)
  bq_suffix=$(date -d "$day" +%Y%m%d)

  echo "Uploading $table: $day..."
  psql  -c "\\copy (select $sel from $table where created_at >= '$day' and created_at < '$next_day') TO '$table-$day.csv' WITH CSV HEADER"
  gzip $table-$day.csv
  bq load --allow_quoted_newlines --project_id  --replace --source_format=CSV --autodetect --max_bad_records 100 .$table$bq_suffix $table-$day.csv.gz
  rm $table-$day.csv.gz
};

function upload_table {
  t=$1
  s=$2
  start_date=$3
  end_date=$4
  while [ "$start_date" != "$end_date" ]; do
       upload_day "$t" "$s" "$start_date"
       start_date=$(date -d "$start_date+1 days" +%Y-%m-%d)
  done
}

upload_table "$1" '*' "$2" "$3"



#//Runs ETL job for non-daemon mode
#func pgEtlManualJob (table,columnSelect, startDate,endDate string){
#	config.CheckCmdExists("psql")
#	config.LoadEnv()
#
#	//Pg options
#	tblExport := "'"+table+"-"+startDate+".csv'"
#	dayStart := "'"+startDate+"'"
#	dayEnd := "'"+endDate+"'"
#
#	log.Info("MANUAL WORKFLOW STARTED :::::: Downloading table: "+ table +":" + startDate + "...")
#	cmd := exec.Command("psql","-c", "\\copy (select "+columnSelect+" from "+table+" where created_at >= "+dayStart+" and created_at < "+dayEnd+") TO "+tblExport+" WITH CSV HEADER")
#	out, err := cmd.CombinedOutput()
#	if err != nil {
#		log.Error("PSQL COMMAND: failed with %s\n", err)
#	}
#	log.Fatalf("ERROR:\n%s\n", string(out))
#
#}
