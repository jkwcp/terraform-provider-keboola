package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

// Delete function to the resource AWS S3 Bucket Extractor Rows.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3ExtractorRows() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAWSS3ExtractorRowsCreate,
		Read:   resourceKeboolaAWSS3ExtractorRowsRead,
		Update: resourceKeboolaAWSS3ExtractorRowsUpdate,
		Delete: resourceKeboolaAWSS3ExtractorRowsDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},
		Schema: map[string]*schema.Schema{
			"extractor_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"bucket": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"key": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"include_subfolders": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"new_files_only": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
		},
	}

}

// Create function to the resource AWS S3 Bucket Extractor Rows.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3ExtractorRowsCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating S3 Bucket Extractor Rows in Keboola")

	extractorID := d.Get("extractor_id").(string)

	s3BucketExtractorRowsConfig := S3BucketExtractorRowsConfig{
		Parameters: S3BucketExtractorRowsConfigParameter{
			Bucket:            d.Get("bucket").(string),
			Key:               d.Get("key").(string),
			IncludeSubFolders: d.Get("include_subfolders").(bool),
			NewFilesOnly:      d.Get("new_files_only").(bool),
		},
	}

	s3BucketExtractorRowsConfigJSON, err := json.Marshal(s3BucketExtractorRowsConfig)

	if err != nil {
		return err
	}

	createS3BucketRowForm := url.Values{}
	createS3BucketRowForm.Add("name", d.Get("name").(string))
	createS3BucketRowForm.Add("description", d.Get("description").(string))
	createS3BucketRowForm.Add("configuration", string(s3BucketExtractorRowsConfigJSON))

	createS3BucketRowsBuffer := buffer.FromForm(createS3BucketRowForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage(fmt.Sprintf("storage/components/keboola.ex-aws-s3/configs/%s/rows", extractorID), createS3BucketRowsBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	var createResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createResult)

	if err != nil {
		return err
	}

	d.SetId(string(createResult.ID))

	return resourceKeboolaAWSS3ExtractorRowsRead(d, meta)
}

// Read function to the resource AWS S3 Bucket Extractor Rows.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3ExtractorRowsRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading S3 Bucket Extractor Rows in Keboola")
	client := meta.(*KBCClient)

	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-aws-s3/configs/%s/rows/", d.Get("extractor_id")))

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResponse)
	}

	var s3BucketExtractorRows []S3BucketExtractorRows

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&s3BucketExtractorRows)

	if err != nil {
		return err
	}

	for _, row := range s3BucketExtractorRows {
		if row.ID == d.Id() {
			d.Set("id", row.ID)
			d.Set("name", row.Name)
			d.Set("description", row.Description)
			d.Set("bucket", row.Configuration.Parameters.Bucket)
			d.Set("key", row.Configuration.Parameters.Key)
			d.Set("include_subfolders", row.Configuration.Parameters.IncludeSubFolders)
			d.Set("new_files_only", row.Configuration.Parameters.NewFilesOnly)
		}
	}

	return nil
}

// Update function to the resource AWS S3 Bucket Extractor Rows.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3ExtractorRowsUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating S3 Bucket Extractor Rows in Keboola.")

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-aws-s3/configs/%s/rows/%s", d.Get("extractor_id"), d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var s3BucketExtractorRows S3BucketExtractorRows

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&s3BucketExtractorRows)

	if err != nil {
		return err
	}

	extractorID := d.Get("extractor_id").(string)

	s3BucketExtractorRowsConfig := S3BucketExtractorRowsConfig{
		Parameters: S3BucketExtractorRowsConfigParameter{
			Bucket:            d.Get("bucket").(string),
			Key:               d.Get("key").(string),
			IncludeSubFolders: d.Get("include_subfolders").(bool),
			NewFilesOnly:      d.Get("new_files_only").(bool),
		},
	}

	s3BucketExtractorRowsConfigJSON, err := json.Marshal(s3BucketExtractorRowsConfig)

	if err != nil {
		return err
	}

	updateS3BucketRowForm := url.Values{}
	updateS3BucketRowForm.Add("name", d.Get("name").(string))
	updateS3BucketRowForm.Add("description", d.Get("description").(string))
	updateS3BucketRowForm.Add("configuration", string(s3BucketExtractorRowsConfigJSON))

	updateS3BucketRowsBuffer := buffer.FromForm(updateS3BucketRowForm)

	createResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-aws-s3/configs/%s/rows/%s", extractorID, d.Id()), updateS3BucketRowsBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	return resourceKeboolaAWSS3ExtractorRowsRead(d, meta)
}

// Delete function to the resource AWS S3 Bucket Extractor Rows.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3ExtractorRowsDelete(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Deleting S3 Bucket Extractor Rows in Keboola.")

	extractorID := d.Get("extractor_id").(string)

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-aws-s3/configs/%s/rows/%s", extractorID, d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
