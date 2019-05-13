package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaAWSS3Bucket() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAWSS3BucketTablesCreate,
		Read:   resourceKeboolaAWSS3BucketTablesRead,
		Update: resourceKeboolaAWSS3BucketTablesUpdate,
		Delete: resourceKeboolaAWSS3BucketTablesDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"table_id": {
				Type:     schema.TypeString,
				Required: true,
			},
		},
	}
}
func resourceKeboolaAWSS3BucketTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating AWS S3 Writer Tables in Keboola.")

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("table_id").(string)

	storageTables := make([]AWSs3WriterStorageTable, 0, len(tableID))

	beforeStorage := make([]Awss3WriterStorageTableBefore, 0, len(tableID))

	var awss3Writer AWSs3Writer
	storageTable := AWSs3WriterStorageTable{
		Source:      tableID,
		Destination: fmt.Sprintf("%s.csv", tableID),
	}
	storageTables = append(storageTables, storageTable)

	component := Awss3Definition{
		Component: "keboola.processor-move-files",
	}

	direction := Awss3parameters{
		Direction: "files",
	}

	bef := Awss3WriterStorageTableBefore{
		DirectionStorage: direction,
		ComponentStorage: component,
	}

	beforeStorage = append(beforeStorage, bef)

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", writerID))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&awss3Writer)

	if err != nil {
		return err
	}

	awss3Writer.Configuration.Storage.Input.Tables = storageTables
	awss3Writer.Configuration.Processors.Before = beforeStorage
	awss3WriterConfigJSON, err := json.Marshal(awss3Writer.Configuration)

	if err != nil {
		return err
	}

	updateAwsS3Form := url.Values{}
	updateAwsS3Form.Add("configuration", string(awss3WriterConfigJSON))
	updateAwsS3Form.Add("changeDescription", "Update S3 Bucket tables")

	updateAwsS3Buffer := buffer.FromForm(updateAwsS3Form)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", writerID), updateAwsS3Buffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	d.SetId(writerID)
	return nil
}
func resourceKeboolaAWSS3BucketTablesRead(d *schema.ResourceData, meta interface{}) error {
	/*
		log.Println("[INFO] Reading AWS S3 Bucket Writer Tables from Keboola.")

		if d.Id() == "" {
			return nil
		}

		client := meta.(*KBCClient)

		getAWSS3BucketTabletWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", d.Id()))

		if hasErrors(err, getAWSS3BucketTabletWriterResponse) {
			if getAWSS3BucketTabletWriterResponse.StatusCode == 404 {
				d.SetId("")
				return nil
			}

			return extractError(err, getAWSS3BucketTabletWriterResponse)
		}

		var awss3BucketshiftWriter AWSs3Writer

		decoder := json.NewDecoder(getAWSS3BucketTabletWriterResponse.Body)
		err = decoder.Decode(&awss3BucketshiftWriter)

		if err != nil {
			return err
		}

		var tables []map[string]interface{}
		storageInputTableMap := make(map[string]AWSs3WriterStorageTable)

		for _, storageInputTable := range awss3BucketshiftWriter.Configuration.Storage.Input.Tables {
			storageInputTableMap[storageInputTable.Source] = storageInputTable
		}
		for _, tableConfig := range awsredshiftWriter.Configuration.Parameters.Tables {
			storageInputTable := storageInputTableMap[tableConfig.TableID]
			tableDetails := map[string]interface{}{

				"changed_since":  storageInputTable.ChangedSince,
				"where_column":   storageInputTable.WhereColumn,
				"where_operator": storageInputTable.WhereOperator,
				"where_values":   storageInputTable.WhereValues,
			}

		}

		d.Set("table", tables)
	*/
	return nil
}
func resourceKeboolaAWSS3BucketTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating AWS S3 Writer Tables in Keboola.")

	return nil
}

func resourceKeboolaAWSS3BucketTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating AWS S3 Writer Tables in Keboola.")
	return nil
}
