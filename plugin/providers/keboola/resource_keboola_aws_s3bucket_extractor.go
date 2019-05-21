package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//Not complete, missing processors

//region Keboola API Contracts

type S3BucketExtractorConfigurationParameters struct {
	AccessKeyId              string `json:"accessKeyId,omitempty"`
	EncryptedAccessKeySecret string `json:"#secretAccessKey,omitempty"`
}

type S3BucketExtractorConfiguration struct {
	Parameters S3BucketExtractorConfigurationParameters `json:"parameters"`
}

type AWSS3Extractor struct {
	ID            string                         `json:"id"`
	Name          string                         `json:"name"`
	Description   string                         `json:"description"`
	Configuration S3BucketExtractorConfiguration `json:"configuration"`
	RowsSortOrder []string                       `json:"rowsSortOrder"`
	//Rows
}

type S3BucketExtractorRowsConfigParameter struct {
	Bucket            string `json:"bucket"`
	Key               string `json:"key"`
	IncludeSubFolders bool   `json: "includeSubfolders"`
	NewFilesOnly      bool   `json: "newFilesOnly"`
}

type S3BucketExtractorRowsConfig struct {
	Parameters S3BucketExtractorRowsConfigParameter `json:"parameters, omitempty"`
}

type S3BucketExtractorRows struct {
	ID            string                      `json:"id"`
	Name          string                      `json:"name"`
	Description   string                      `json:"description"`
	Configuration S3BucketExtractorRowsConfig `json:"configuration"`
}

// Main function to the resource AWS S3 Bucket Extractor.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3Extractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAWSS3ExtractorCreate,
		Read:   resourceKeboolaAWSS3ExtractorRead,
		Update: resourceKeboolaAWSS3ExtractorUpdate,
		Delete: resourceKeboolaAWSS3ExtractorDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"access_key_id": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"access_key": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

// Create function to the resource AWS S3 Bucket Extractor.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3ExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating S3Extractor Bucket in Keboola")

	s3BucketExtractorConfiguration := S3BucketExtractorConfiguration{
		Parameters: S3BucketExtractorConfigurationParameters{
			AccessKeyId:              d.Get("access_key_id").(string),
			EncryptedAccessKeySecret: d.Get("access_key").(string),
		},
	}

	s3BucketExtractorConfigJSON, err := json.Marshal(s3BucketExtractorConfiguration)

	if err != nil {
		return err
	}

	createS3BucketForm := url.Values{}
	createS3BucketForm.Add("name", d.Get("name").(string))
	createS3BucketForm.Add("description", d.Get("description").(string))
	createS3BucketForm.Add("configuration", string(s3BucketExtractorConfigJSON))

	createS3BucketBuffer := buffer.FromForm(createS3BucketForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage("storage/components/keboola.ex-aws-s3/configs", createS3BucketBuffer)

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

	return resourceKeboolaAWSS3ExtractorRead(d, meta)
}

// Read function to the resource AWS S3 Bucket Extractor.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3ExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading AWS S3 Extractor from Keboola.")

	client := meta.(*KBCClient)
	getAWSS3BucketExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-aws-s3/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getAWSS3BucketExtractorResponse) {
		if getAWSS3BucketExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getAWSS3BucketExtractorResponse)
	}

	var aWSS3Extractor AWSS3Extractor

	decoder := json.NewDecoder(getAWSS3BucketExtractorResponse.Body)
	err = decoder.Decode(&aWSS3Extractor)

	d.Set("id", aWSS3Extractor.ID)
	d.Set("name", aWSS3Extractor.Name)
	d.Set("description", aWSS3Extractor.Description)
	d.Set("access_key_id", aWSS3Extractor.Configuration.Parameters.AccessKeyId)
	d.Set("access_key", aWSS3Extractor.Configuration.Parameters.EncryptedAccessKeySecret)

	if err != nil {
		return err
	}
	return nil
}

// Update function to the resource AWS S3 Bucket Extractor.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3ExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating AWS S3 Extractor in Keboola.")

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-aws-s3/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var aWSS3Extractor AWSS3Extractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&aWSS3Extractor)

	if err != nil {
		return err
	}

	s3BucketExtractorConfiguration := S3BucketExtractorConfiguration{
		Parameters: S3BucketExtractorConfigurationParameters{
			AccessKeyId:              d.Get("access_key_id").(string),
			EncryptedAccessKeySecret: d.Get("access_key").(string),
		},
	}

	s3BucketExtractorConfigJSON, err := json.Marshal(s3BucketExtractorConfiguration)

	if err != nil {
		return err
	}

	updateS3BucketForm := url.Values{}
	updateS3BucketForm.Add("name", d.Get("name").(string))
	updateS3BucketForm.Add("description", d.Get("description").(string))
	updateS3BucketForm.Add("configuration", string(s3BucketExtractorConfigJSON))

	updateS3BucketFormBuffer := buffer.FromForm(updateS3BucketForm)
	updateS3BucketResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-aws-s3/configs/%s", d.Id()), updateS3BucketFormBuffer)

	if hasErrors(err, updateS3BucketResponse) {
		return extractError(err, updateS3BucketResponse)
	}

	return resourceKeboolaAWSS3ExtractorRead(d, meta)
}

// Delete function to the resource AWS S3 Bucket Extractor.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaAWSS3ExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting AWS S3 Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-aws-s3/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
