package keboola

//4900
// this isn't complete can't update. Can Create Tables and delete
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
			"auto_run": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"name": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"s3_row_parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"prefix": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
		},
	}
}
func resourceKeboolaAWSS3BucketTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating AWS S3 Writer Tables in Keboola.")

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("table_id").(string)

	procosser := configurationOfRows(tableID)
	storageTables := configurationOfStorageTable(tableID)

	client := meta.(*KBCClient)

	var awss3Writer AWSs3Writer

	getWriterResponseRowStorage, err := client.GetFromRowStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", writerID))
	if hasErrors(err, getWriterResponseRowStorage) {
		return extractError(err, getWriterResponseRowStorage)
	}
	decoderRowStorage := json.NewDecoder(getWriterResponseRowStorage.Body)
	err = decoderRowStorage.Decode(&awss3Writer)

	if err != nil {
		return err
	}

	awss3Writer.ConfigurationRow.RowsInfo.Storage.Input.Tables = storageTables
	awss3Writer.ConfigurationRow.RowsInfo.Processor = procosser

	awss3Row := d.Get("s3_row_parameters").(map[string]interface{})
	awss3Writer.ConfigurationRow.RowsInfo.Parameters, err = mapAWSs3CredentialsToRowConfiguration(awss3Row, client)
	if err != nil {
		return err
	}

	awss3Writer.ConfigurationRow.RowsInfo.name = d.Get("name").(string)

	awss3WriterConfigJSON, err := json.Marshal(awss3Writer.ConfigurationRow.RowsInfo)
	if err != nil {
		return err
	}

	updateAwsS3Form := url.Values{}
	updateAwsS3Form.Add("configuration", string(awss3WriterConfigJSON))

	updateAwsS3Form.Add("changeDescription", "Update S3 Bucket tables")

	updateAwsS3Buffer := buffer.FromForm(updateAwsS3Form)
	createResponse, err := client.PostToStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s/rows", writerID), updateAwsS3Buffer)
	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	d.SetId(writerID)
	return resourceKeboolaAWSS3BucketTablesRead(d, meta)
}
func configurationOfStorageTable(tableID string) []AWSs3WriterStorageTable {

	storageTables := make([]AWSs3WriterStorageTable, 0, len(tableID))

	storageTable := AWSs3WriterStorageTable{
		Source: tableID,

		Destination: fmt.Sprintf("%s.csv", tableID),
	}
	storageTables = append(storageTables, storageTable)
	return storageTables

}
func configurationOfRows(tableID string) (Processer AWSs3WriterProcessor) {

	component := Awss3Definition{
		Component: "keboola.processor-move-files",
	}

	direction := Awss3parameters{
		Direction: "files",
	}

	beforeStorage := make([]Awss3WriterStorageTableBefore, 0, len(tableID))

	bef := Awss3WriterStorageTableBefore{
		DirectionStorage: direction,
		ComponentStorage: component,
	}

	beforeStorage = append(beforeStorage, bef)

	Procosser := AWSs3WriterProcessor{
		Before: beforeStorage,
	}
	return Procosser

}
func mapAWSs3CredentialsToRowConfiguration(source map[string]interface{}, client *KBCClient) (AWSs3WriterDatabaseRowParameters, error) {
	Parameters := AWSs3WriterDatabaseRowParameters{}
	var err error
	err = nil
	if val, ok := source["prefix"]; ok {
		Parameters.Prefix = val.(string)
	}

	return Parameters, err
}

func resourceKeboolaAWSS3BucketTablesRead(d *schema.ResourceData, meta interface{}) error {

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

	var awss3Writer AWSs3Writer
	decoder := json.NewDecoder(getAWSS3BucketTabletWriterResponse.Body)
	err = decoder.Decode(&awss3Writer)

	getWriterResponseRowStorage, err := client.GetFromRowStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", d.Id()))
	if hasErrors(err, getWriterResponseRowStorage) {
		if getWriterResponseRowStorage.StatusCode == 404 {
			d.SetId("")
			return nil
		}
		return extractError(err, getWriterResponseRowStorage)
	}

	var awss3BucketshiftWriter AWSs3Writer

	rowStoragedecoder := json.NewDecoder(getWriterResponseRowStorage.Body)
	err = rowStoragedecoder.Decode(&awss3BucketshiftWriter)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}
	storageInputTableMap := make(map[string]AWSs3WriterStorageTable)

	for _, storageInputTable := range awss3BucketshiftWriter.ConfigurationRow.RowsInfo.Storage.Input.Tables {
		storageInputTableMap[storageInputTable.Source] = storageInputTable
	}
	if d.Get("provision_new_database") == false {
		dbParameters := make(map[string]interface{})

		databaseCredentials := awss3BucketshiftWriter.ConfigurationRow.RowsInfo.Parameters

		dbParameters["prefix"] = databaseCredentials.Prefix

		d.Set("s3_row_parameters", dbParameters)
	}
	d.Set("tables", tables)
	if d.Get("auto_run") == true {

		awss3ConfigRunResponse, err := client.PostToDockerRun("keboola.wr-aws-s3", d.Id())
		if hasErrors(err, awss3ConfigRunResponse) {
			return extractError(err, awss3ConfigRunResponse)
		}
	}
	return nil
}

func resourceKeboolaAWSS3BucketTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating AWS S3 Writer Tables in Keboola.")

	return nil

}

func resourceKeboolaAWSS3BucketTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Deleting AWS S3 Writer Tables in Keboola.")

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")
	return nil
}
