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

//Adform Extractor
type AdformExtractor struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

//endregion

func resourceKeboolaAdformExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAdformExtractorCreate,
		Read:   resourceKeboolaAdformExtractorRead,
		Update: resourceKeboolaAdformExtractorUpdate,
		Delete: resourceKeboolaAdformExtractorDelete,

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

//Creates the Adform Extractor on Keboola Connection
//Called when new Adform extractor added to the terraform
func resourceKeboolaAdformExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Adform Extractor in Keboola.")

	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", d.Get("name").(string))
	createExtractorForm.Add("description", d.Get("description").(string))

	createExtractorBuffer := buffer.FromForm(createExtractorForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage("storage/components/keboola.ex-adform/configs", createExtractorBuffer)

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
func resourceKeboolaAdformExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Adform Extractor from Keboola.")

	client := meta.(*KBCClient)
	getAdformExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-adform/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getAdformExtractorResponse) {
		if getAdformExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getAdformExtractorResponse)
	}

	var adformExtractor AdformExtractor

	decoder := json.NewDecoder(getAdformExtractorResponse.Body)
	err = decoder.Decode(&adformExtractor)

	if err != nil {
		return err
	}

	d.Set("id", adformExtractor.ID)
	d.Set("name", adformExtractor.Name)
	d.Set("description", adformExtractor.Description)

	return nil
}

//Updates component configuration on Keboola Connection
//Called when local terraform was changed
//Complete
func resourceKeboolaAdformExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Adform Extractor in Keboola.")

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-adform/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var adformExtractor AdformExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&adformExtractor)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("changeDescription", "Updated Adform Extractor configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)
	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-adform/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return nil
}

//Destroy a Adform extractor
//Called when the components is removed from terraform
//Complete
func resourceKeboolaAdformExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Adform Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-adform/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
