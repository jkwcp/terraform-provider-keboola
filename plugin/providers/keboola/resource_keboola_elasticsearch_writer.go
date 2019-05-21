package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//region Keboola API Contracts

type ElasticSearchWriterTableItem struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   bool   `json:"nullable"`
	DefaultValue string `json:"default"`
}

//Structure of the ElasticSearchWriter
type ElasticSearchWriter struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

//Specifies the Create, Read, Update, and Delete functions for the ElasticSearch Writer
//Specified in and called when provider.go is ran
//Functionality is complete
func resourceKeboolaElasticSearchWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaElasticSearchWriterCreate,
		Read:   resourceKeboolaElasticSearchWriterRead,
		Update: resourceKeboolaElasticSearchWriterUpdate,
		Delete: resourceKeboolaElasticSearchWriterDelete,
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
		},
	}
}

//Creates the ElasticSearch Writer resource in Keboola Connection project
//Called from main.tf using "terraform apply" command
//Function is completed
func resourceKeboolaElasticSearchWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating ElasticSearch Writer in Keboola.")

	client := meta.(*KBCClient)

	createdConfigID, err := createElasticSearchWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetId(createdConfigID)

	return resourceKeboolaElasticSearchWriterRead(d, meta)
}

//Configures the ElasticSearch Writer resource in Keboola Connection project
//Called from main.tf using "terraform apply" command along with resourceKeboolaElasticSearchWriterCreate
//Function is functional but more configurations can be added
func createElasticSearchWriterConfiguration(name string, description string, client *KBCClient) (createdID string, err error) {
	form := url.Values{}
	form.Add("name", name)
	form.Add("description", description)

	formdataBuffer := buffer.FromForm(form)

	createWriterConfigResp, err := client.PostToStorage("storage/components/tde-exporter/configs", formdataBuffer)

	if err != nil {
		return "", err
	}

	if hasErrors(err, createWriterConfigResp) {
		return "", extractError(err, createWriterConfigResp)
	}

	var createRes CreateResourceResult

	createDecoder := json.NewDecoder(createWriterConfigResp.Body)
	err = createDecoder.Decode(&createRes)

	if err != nil {
		return "", err
	}

	return string(createRes.ID), nil
}

//Reads the current ElasticSearch Writer within the Keboola project. If the configuration is different, updates it to the configurations specified in main.tf
//Called from main.tf using "terraform apply" command
//Function is completed
func resourceKeboolaElasticSearchWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading ElasticSearch Writers from Keboola.")

	client := meta.(*KBCClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/tde-exporter/configs/%s", d.Id()))
	if d.Id() == "" {
		return nil
	}
	if hasErrors(err, getResp) {
		if getResp.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResp)
	}

	var elasticSearchWriter ElasticSearchWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&elasticSearchWriter)

	if err != nil {
		return err
	}

	d.Set("id", elasticSearchWriter.ID)
	d.Set("name", elasticSearchWriter.Name)
	d.Set("description", elasticSearchWriter.Description)

	return nil
}

//Updates the ElasticSearch Writer resource in Keboola Connection project
//Called from main.tf using "terraform apply" command
//Function is completed
func resourceKeboolaElasticSearchWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating ElasticSearch Writer in Keboola.")

	client := meta.(*KBCClient)

	putResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-elasticsearch/configs/%s", d.Id()))

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}
	var elasticSearchWriter ElasticSearchWriter

	decoder := json.NewDecoder(putResp.Body)
	err = decoder.Decode(&elasticSearchWriter)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)
	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/tde-exporter/configs/%s", d.Id()), updateCredentialsBuffer)
	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}
	return resourceKeboolaElasticSearchWriterRead(d, meta)
}

//Deletes the ElasticSearch Writer resource in Keboola Connection project when the resource specifications are removed from main.tf
//Called from main.tf using "terraform apply" command
//Function is completed
func resourceKeboolaElasticSearchWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting ElasticSearch Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	delFromStorageResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/tde-exporter/configs/%s", d.Id()))

	if hasErrors(err, delFromStorageResp) {
		return extractError(err, delFromStorageResp)
	}

	d.SetId("")

	return nil
}
