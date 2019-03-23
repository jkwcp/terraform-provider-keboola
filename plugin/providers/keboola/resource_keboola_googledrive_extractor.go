package keboola

import (
	"encoding/json"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//region Keboola API Contracts

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

func resourceGoogleDriveExtractorRead(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func resourceGoogleDriveExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func resourceGoogleDriveExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	return nil
}
