package keboola

import (
	"encoding/json"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//region Keboola API Contracts

/*
type SnowflakeExtractorDatabaseParameters struct {
	HostName          string `json:"host"`
	Database          string `json:"database"`
	Password          string `json:"password,omitempty"`
	EncryptedPassword string `json:"#password,omitempty"`
	Username          string `json:"user"`
	Schema            string `json:"schema"`
	Port              string `json:"port"`
	Driver            string `json:"driver"`
	Warehouse         string `json:"warehouse"`
}


type SnowflakeWriterConfiguration struct {
	Parameters SnowflakeWriterParameters `json:"parameters"`
	Storage    SnowflakeWriterStorage    `json:"storage,omitempty"`
}
*/
type SnowFlakeExtractor struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

//endregion

func resourceKeboolaSnowlakeExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceSnowFlakeExtractorCreate,
		Read:   resourceSnowFlakeExtractorRead,
		Update: resourceSnowFlakeExtractorUpdate,
		Delete: resourceSnowFlakeExtractorDelete,

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

func resourceSnowFlakeExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Google Drive Extractor in Keboola.")

	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", d.Get("name").(string))
	createExtractorForm.Add("description", d.Get("description").(string))

	createExtractorBuffer := buffer.FromForm(createExtractorForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage("storage/components/keboola.ex-db-snowflake/configs", createExtractorBuffer)

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

func resourceSnowFlakeExtractorRead(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func resourceSnowFlakeExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func resourceSnowFlakeExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	return nil
}
