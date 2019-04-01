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

type TableauWriter struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

func resourceKeboolaTableauWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaTableauWriterCreate,
		Read:   resourceKeboolaTableauWriterRead,
		Update: resourceKeboolaTableauWriterUpdate,
		Delete: resourceKeboolaTableauWriterDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			// "writer_id": {
			// 	Type:     schema.TypeString,
			// 	Required: true,
			// },
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

func resourceKeboolaTableauWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Tableau Writer in Keboola.")

	writerID := d.Get("writer_id").(string)
	client := meta.(*KBCClient)

	createdConfigID, err := createTableauWriterConfiguration(writerID, d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetId(createdConfigID)

	return resourceKeboolaTableauWriterRead(d, meta)
}

func createTableauWriterConfiguration(writerID string, name string, description string, client *KBCClient) (createdID string, err error) {
	form := url.Values{}
	form.Add("name", name)
	form.Add("description", description)

	formdataBuffer := buffer.FromForm(form)

	createWriterConfigResp, err := client.PutToStorage(fmt.Sprintf("storage/components/tde-exporter/configs/%s", writerID), formdataBuffer)

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

func resourceKeboolaTableauWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Tableau Writers from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/tde-exporter/configs/%s", d.Id()))

	if hasErrors(err, getResp) {
		if getResp.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResp)
	}

	var tableauWriter TableauWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&tableauWriter)

	if err != nil {
		return err
	}

	d.Set("id", tableauWriter.ID)
	d.Set("name", tableauWriter.Name)
	d.Set("description", tableauWriter.Description)

	return nil
}

func resourceKeboolaTableauWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Tableau Writer in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	client := meta.(*KBCClient)
	formdataBuffer := buffer.FromForm(form)
	putResp, err := client.PutToStorage(fmt.Sprintf("storage/components/tde-exporter/configs/%s", d.Id()), formdataBuffer)

	if err != nil {
		return err
	}

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	return resourceKeboolaTableauWriterRead(d, meta)
}

func resourceKeboolaTableauWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Tableau Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	delFromSyrupResp, err := client.DeleteFromSyrup(fmt.Sprintf("tde-exporter/configs/%s", d.Id()))

	if hasErrors(err, delFromSyrupResp) {
		return extractError(err, delFromSyrupResp)
	}

	delFromStorageResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/tde-exporter/configs/%s", d.Id()))

	if hasErrors(err, delFromStorageResp) {
		return extractError(err, delFromStorageResp)
	}

	d.SetId("")

	return nil
}
