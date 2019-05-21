package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

type EmailAttachmentConfigParameters struct {
	Email       string   `json:"email"`
	Delimiter   string   `json:"delimiter"`
	Enclosure   string   `json:"enclosure"`
	PrimaryKey  []string `json:"primaryKey"`
	Incremental bool     `json:"incremental"`
}

type EmailAttachmentConfig struct {
	Parameters EmailAttachmentConfigParameters `json:"parameters"`
}

type EmailAttachmentExtractor struct {
	ID            string                `json:"id"`
	Name          string                `json:"name"`
	Description   string                `json:"description"`
	Configuration EmailAttachmentConfig `json:"configuration"`
}

type GetEmailResponse struct {
	Email string `json:"email"`
}

type GetEmailRequest struct {
	ConfigData struct {
		Parameters struct {
			Config string `json:"config"`
		} `json:"paramters"`
	} `json:"configData"`
}

func resourceKeboolaEmailAttachmentExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaEmailAttachmentExtractorCreate,
		Read:   resourceKeboolaEmailAttachmentExtractorRead,
		Update: resourceKeboolaEmailAttachmentExtractorUpdate,
		Delete: resourceKeboolaEmailAttachmentExtractorDelete,
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
			"delimiter": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  ",",
			},
			"enclosure": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "\"",
			},
			"incremental": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"primary_key": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
		},
	}
}

func resourceKeboolaEmailAttachmentExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Email Attachment Extractor in Keboola")

	client := meta.(*KBCClient)
	d.Partial(true)

	createdEmailAttachmentExtractorID, err := createEmailAttachmentExtractorConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetPartial("name")
	d.SetPartial("description")

	getEmailAddressBuffer := fmt.Sprintf("{ \"configData\": { \"parameters\": { \"config\" : \"%v\"}}}", createdEmailAttachmentExtractorID)

	emailAddress, err := getEmailAddress(getEmailAddressBuffer, client)

	if err != nil {
		return err
	}

	emailAttachmentConfig := EmailAttachmentConfig{
		Parameters: EmailAttachmentConfigParameters{
			Email:       emailAddress,
			Delimiter:   d.Get("delimiter").(string),
			Enclosure:   d.Get("enclosure").(string),
			Incremental: d.Get("incremental").(bool),
		},
	}

	emailAttachmentConfigurationJSON, err := json.Marshal(emailAttachmentConfig)

	if err != nil {
		return err
	}

	updateConfigurationRequestForm := url.Values{}
	updateConfigurationRequestForm.Add("configuration", string(emailAttachmentConfigurationJSON))
	updateConfigurationRequestForm.Add("changeDescription", "Created database credentials")

	updateConfigurationRequestBuffer := buffer.FromForm(updateConfigurationRequestForm)

	updateConfigurationResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-email-attachments/configs/%s", createdEmailAttachmentExtractorID), updateConfigurationRequestBuffer)

	if hasErrors(err, updateConfigurationResponse) {
		return extractError(err, updateConfigurationResponse)
	}

	d.SetPartial("delimiter")
	d.SetPartial("enclosure")
	d.SetPartial("incremental")

	d.SetId(createdEmailAttachmentExtractorID)
	d.Partial(false)

	return resourceKeboolaEmailAttachmentExtractorRead(d, meta)
}

func createEmailAttachmentExtractorConfiguration(name string, description string, client *KBCClient) (createdEmailAttachmentExtractorID string, err error) {
	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", name)
	createExtractorForm.Add("description", description)

	createExtractorBuffer := buffer.FromForm(createExtractorForm)
	createResponse, err := client.PostToStorage("storage/components/keboola.ex-email-attachments/configs", createExtractorBuffer)

	if hasErrors(err, createResponse) {
		return "", extractError(err, createResponse)
	}

	var createExtractorResult CreateResourceResult
	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createExtractorResult)

	if err != nil {
		return "", err
	}

	return string(createExtractorResult.ID), nil
}

func getEmailAddress(emailExtractorID string, client *KBCClient) (defaultEmailAddress string, err error) {

	body := []byte(emailExtractorID)

	getResponseEmail, err := client.PostToDockerAction("keboola.ex-email-attachments/action/get", body)

	if hasErrors(err, getResponseEmail) {
		return "", extractError(err, getResponseEmail)
	}

	var getEmailResponse GetEmailResponse

	decoder := json.NewDecoder(getResponseEmail.Body)
	err = decoder.Decode(&getEmailResponse)

	if err != nil {
		return "", err
	}

	return string(getEmailResponse.Email), nil
}

func resourceKeboolaEmailAttachmentExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading EmailAttachment Extractor from Keboola.")

	client := meta.(*KBCClient)
	getEmailAttachmentExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-email-attachments/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getEmailAttachmentExtractorResponse) {
		if getEmailAttachmentExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getEmailAttachmentExtractorResponse)
	}

	var emailAttachmentExtractor EmailAttachmentExtractor

	decoder := json.NewDecoder(getEmailAttachmentExtractorResponse.Body)
	err = decoder.Decode(&emailAttachmentExtractor)

	if err != nil {
		return err
	}

	d.Set("id", emailAttachmentExtractor.ID)
	d.Set("name", emailAttachmentExtractor.Name)
	d.Set("description", emailAttachmentExtractor.Description)
	d.Set("delimiter", emailAttachmentExtractor.Configuration.Parameters.Delimiter)
	d.Set("enclosure", emailAttachmentExtractor.Configuration.Parameters.Enclosure)
	d.Set("incremental", emailAttachmentExtractor.Configuration.Parameters.Incremental)

	runExtractorResponse, err := client.PostToDockerRun("keboola.ex-email-attachments", d.Id())

	if hasErrors(err, runExtractorResponse) {
		return extractError(err, runExtractorResponse)
	}

	return nil
}
func resourceKeboolaEmailAttachmentExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating EmailAttachment Extractor in Keboola.")

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-email-attachments/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var emailAttachmentExtractor EmailAttachmentExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&emailAttachmentExtractor)

	if err != nil {
		return err
	}

	emailAttachmentConfig := EmailAttachmentConfig{
		Parameters: EmailAttachmentConfigParameters{
			Email:       emailAttachmentExtractor.Configuration.Parameters.Email,
			Delimiter:   d.Get("delimiter").(string),
			Enclosure:   d.Get("enclosure").(string),
			Incremental: d.Get("incremental").(bool),
		},
	}

	emailAttachmentConfigurationJSON, err := json.Marshal(emailAttachmentConfig)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("configuration", string(emailAttachmentConfigurationJSON))
	updateCredentialsForm.Add("changeDescription", "Created database credentials")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)

	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-email-attachments/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return resourceKeboolaEmailAttachmentExtractorRead(d, meta)
}
func resourceKeboolaEmailAttachmentExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Email Attachment Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-email-attachments/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
