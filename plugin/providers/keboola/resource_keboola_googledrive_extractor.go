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

//Google Drive Extractor
//Incomplete: needs to add OAuth2 access token for google drive
type GoogleDriveExtractor struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description`
}

//endregion

func resourceGoogleDriveExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceGoogleDriveExtractorCreate,
		Read:   resourceGoogleDriveExtractorRead,
		Update: resourceGoogleDriveExtractorUpdate,
		Delete: resourceGoogleDriveExtractorDelete,

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

//Creates the GoogleDrive Extractor on Keboola Connection
//Called when new GoogleDrive extractor added to the terraform
//Incomplete: needs to add OAuth 2 token from google api
func resourceGoogleDriveExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Google Drive Extractor in Keboola.")

	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", d.Get("name").(string))
	createExtractorForm.Add("description", d.Get("description").(string))

	createExtractorBuffer := buffer.FromForm(createExtractorForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage("storage/components/keboola.ex-google-drive/configs", createExtractorBuffer)

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
func resourceGoogleDriveExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading GoogleDrive Extractor from Keboola.")

	client := meta.(*KBCClient)
	getGoogleDriveExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-google-drive/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getGoogleDriveExtractorResponse) {
		if getGoogleDriveExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getGoogleDriveExtractorResponse)
	}

	var googleDriveExtractor GoogleDriveExtractor

	decoder := json.NewDecoder(getGoogleDriveExtractorResponse.Body)
	err = decoder.Decode(&googleDriveExtractor)

	if err != nil {
		return err
	}

	d.Set("id", googleDriveExtractor.ID)
	d.Set("name", googleDriveExtractor.Name)
	d.Set("description", googleDriveExtractor.Description)

	return nil
}

//Updates component configuration on Keboola Connection
//Called when local terraform was changed
//Complete
func resourceGoogleDriveExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating GoogleDrive Extractor in Keboola.")

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-google-drive/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var googleDriveExtractor GoogleDriveExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&googleDriveExtractor)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("changeDescription", "Updated GoogleDrive Extractor configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)
	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-google-drive/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return nil
}

//Destroy a Google Drive extractor
//Called when the components is removed from terraform
//Complete
func resourceGoogleDriveExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting GoogleDrive Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-google-drive/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
