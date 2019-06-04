//https://cloud.google.com/bigquery/docs/loading-data-local
//https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries
package adapters

import (
	"context"
	"github.com/masterxavierfox/goetl-postgres-bigquery/config"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
	"os"

	"cloud.google.com/go/bigquery"
)


func bQUpload(datasetID, tableID, filename string) error{
	config.LoadEnv()
	credsFile := config.GOOGLE_APPLICATION_CREDENTIALS
	//creds, err := ioutil.ReadFile(credsFile)
	//if err != nil {
	//	log.Fatalf("Could note parse config file.")
	//}

	log.Info(":::: ========== BIG QUERY UPLOAD STARTED ========= ::::")
	log.Info("\n -::Dataset ID | ",datasetID,"\n -::Table ID | ",tableID,"\n -::CSV File | ",filename,"\n -::PROJECT ID | ",config.BQ_PROJECT_ID,"\n")
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, config.BQ_PROJECT_ID,option.WithCredentialsFile(credsFile))

	if err != nil {
		return err
	}
	// [START big-query_load_from_file]

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	source := bigquery.NewReaderSource(f)
	source.AutoDetect = true   // Allow BigQuery to determine schema.
	source.SkipLeadingRows = 1 // CSV has a single header line.

	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(source)

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	// [END bigquery_load_from_file]
	DeleteArchive(filename)
	return nil
}
