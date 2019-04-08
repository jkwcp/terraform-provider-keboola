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

type DropboxWriterTableItem struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   bool   `json:"nullable"`
	DefaultValue string `json:"default"`
}

//Structure of the DropboxWriter
type DropboxWriter struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

//Specifies the Create, Read, Update, and Delete functions for the Dropbox Writer
//Specified in and called when provider.go is ran
//Functionality is complete
func resourceKeboolaDropboxWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaDropboxWriterCreate,
		Read:   resourceKeboolaDropboxWriterRead,
		Update: resourceKeboolaDropboxWriterUpdate,
		Delete: resourceKeboolaDropboxWriterDelete,
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

//Creates the Dropbox Writer resource in Keboola Connection project
//Called from main.tf using "terraform apply" command
//Function is completed
func resourceKeboolaDropboxWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Dropbox Writer in Keboola.")

	client := meta.(*KBCClient)

	createdConfigID, err := createDropboxWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetId(createdConfigID)

	return resourceKeboolaDropboxWriterRead(d, meta)
}

//Configures the Dropbox Writer resource in Keboola Connection project
//Called from main.tf using "terraform apply" command along with resourceKeboolaDropboxWriterCreate
//Function is functional but more configurations can be added
func createDropboxWriterConfiguration(name string, description string, client *KBCClient) (createdID string, err error) {
	form := url.Values{}
	form.Add("name", name)
	form.Add("description", description)

	formdataBuffer := buffer.FromForm(form)

	createWriterConfigResp, err := client.PostToStorage("storage/components/keboola.wr-dropbox-v2/configs", formdataBuffer)

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

//Reads the current Dropbox Writer within the Keboola project. If the configuration is different, updates it to the configurations specified in main.tf
//Called from main.tf using "terraform apply" command
//Function is completed
func resourceKeboolaDropboxWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Dropbox Writers from Keboola.")

	client := meta.(*KBCClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-dropbox-v2/configs/%s", d.Id()))
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

	var DropboxWriter DropboxWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&DropboxWriter)

	if err != nil {
		return err
	}

	d.Set("id", DropboxWriter.ID)
	d.Set("name", DropboxWriter.Name)
	d.Set("description", DropboxWriter.Description)

	return nil
}

//Updates the Dropbox Writer resource in Keboola Connection project
//Called from main.tf using "terraform apply" command
//Function is completed
func resourceKeboolaDropboxWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Dropbox Writer in Keboola.")

	client := meta.(*KBCClient)

	putResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-dropbox-v2/configs/%s", d.Id()))

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}
	var DropboxWriter DropboxWriter

	decoder := json.NewDecoder(putResp.Body)
	err = decoder.Decode(&DropboxWriter)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)
	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-dropbox-v2/configs/%s", d.Id()), updateCredentialsBuffer)
	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}
	return resourceKeboolaDropboxWriterRead(d, meta)
}

//Deletes the Dropbox Writer resource in Keboola Connection project when the resource specifications are removed from main.tf
//Called from main.tf using "terraform apply" command
//Function is completed
func resourceKeboolaDropboxWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Dropbox Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	delFromStorageResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-dropbox-v2/configs/%s", d.Id()))

	if hasErrors(err, delFromStorageResp) {
		return extractError(err, delFromStorageResp)
	}

	d.SetId("")

	return nil
}
