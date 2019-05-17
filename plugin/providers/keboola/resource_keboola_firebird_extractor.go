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

//Firebird Extractor
type FirebirdExtractor struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

//endregion
func resourceKeboolaFirebirdExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaFirebirdExtractorCreate,
		Read:   resourceKeboolaFirebirdExtractorRead,
		Update: resourceKeboolaFirebirdExtractorUpdate,
		Delete: resourceKeboolaFirebirdExtractorDelete,

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

//Creates the Firebird Extractor on Keboola Connection
//Called when new Firebird extractor added to the terraform
func resourceKeboolaFirebirdExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Firebird Extractor in Keboola.")

	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", d.Get("name").(string))
	createExtractorForm.Add("description", d.Get("description").(string))

	createExtractorBuffer := buffer.FromForm(createExtractorForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage("storage/components/keboola.ex-db-firebird/configs", createExtractorBuffer)

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

//Reads component configuration from Keboola Connection and compare with local Terraforms,
//updates terraform if changes have been made on Keboola Platform
//Called when update/create is executed
//Complete
func resourceKeboolaFirebirdExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Firebird Extractor from Keboola.")

	client := meta.(*KBCClient)
	getFirebirdExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-firebird/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getFirebirdExtractorResponse) {
		if getFirebirdExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getFirebirdExtractorResponse)
	}

	var firebirdExtractor FirebirdExtractor

	decoder := json.NewDecoder(getFirebirdExtractorResponse.Body)
	err = decoder.Decode(&firebirdExtractor)

	if err != nil {
		return err
	}

	d.Set("id", firebirdExtractor.ID)
	d.Set("name", firebirdExtractor.Name)
	d.Set("description", firebirdExtractor.Description)

	return nil
}

func resourceKeboolaFirebirdExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Firebird Extractor in Keboola.")

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-firebird/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var firebirdExtractor FirebirdExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&firebirdExtractor)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("changeDescription", "Updated Firebird Extractor configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)
	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-firebird/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return nil
}

func resourceKeboolaFirebirdExtractorDelete(d *schema.ResourceData, meta interface{}) error {

	log.Printf("[INFO] Deleting Firebird Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-firebird/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
