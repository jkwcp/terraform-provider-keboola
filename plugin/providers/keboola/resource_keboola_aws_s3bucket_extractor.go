package keboola

import (
	"encoding/json"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

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

	return nil
}

func resourceKeboolaAWSS3ExtractorRead(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func resourceKeboolaAWSS3ExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func resourceKeboolaAWSS3ExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	return nil
}
