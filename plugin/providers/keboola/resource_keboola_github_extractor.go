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

type GithubImportExtractor struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

//endregion

func resourceKeboolaGithubImportExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGithubImportExtractorCreate,
		Read:   resourceKeboolaGithubImportExtractorRead,
		Update: resourceKeboolaGithubImportExtractorUpdate,
		Delete: resourceKeboolaGithubImportExtractorDelete,

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

func resourceKeboolaGithubImportExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Github Import Extractor in Keboola.")

	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", d.Get("name").(string))
	createExtractorForm.Add("description", d.Get("description").(string))

	createExtractorBuffer := buffer.FromForm(createExtractorForm)

	client := meta.(*KBCClient)

	// something should be changed here!
	createResponse, err := client.PostToStorage("storage/components/keboola.ex-github/configs", createExtractorBuffer)

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
	//return resourceKeboolaGithubImportExtractorRead(d, meta)
}

// GitHub Read function
func resourceKeboolaGithubImportExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Github Import Extractor from Keboola.")

	client := meta.(*KBCClient)

	// component link needs to be changed...
	getGitHubExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-github/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	// what defines the getCSVExtractorResponse? Should I have a getGithubExtractor response somewhere?
	if hasErrors(err, getGitHubExtractorResponse) {
		if getGitHubExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getGitHubExtractorResponse)
	}

	// What is this??
	var githubImportExtractor GithubImportExtractor

	decoder := json.NewDecoder(getGitHubExtractorResponse.Body)
	err = decoder.Decode(&githubImportExtractor)

	if err != nil {
		return err
	}

	// what csvImportExtractor??
	d.Set("id", githubImportExtractor.ID)
	d.Set("name", githubImportExtractor.Name)
	d.Set("description", githubImportExtractor.Description)

	return nil
}

func resourceKeboolaGithubImportExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func resourceKeboolaGithubImportExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	return nil
}
