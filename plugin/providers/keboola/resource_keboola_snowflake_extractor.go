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
type SnowflakeExtractorParameters struct {
	Database SnowflakeExtractorDatabaseParameters `json:"db"`
}

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

type SnowflakeExtractorConfiguration struct {
	Parameters SnowflakeExtractorParameters `json:"parameters"`
}

type SnowFlakeExtractor struct {
	ID            string                          `json:"id"`
	Name          string                          `json:"name"`
	Description   string                          `json:"description"`
	Configuration SnowflakeExtractorConfiguration `json:"configuration"`
}

//endregion

func resourceKeboolaSnowlakeExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowFlakeExtractorCreate,
		Read:   resourceKeboolaSnowFlakeExtractorRead,
		Update: resourceKeboolaSnowFlakeExtractorUpdate,
		Delete: resourceKeboolaSnowFlakeExtractorDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"snowflake_db_parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"hostname": {
							Type:     schema.TypeString,
							Required: true,
						},
						"port": {
							Type:     schema.TypeInt,
							Optional: true,
							Default:  443,
						},
						"database": {
							Type:     schema.TypeString,
							Required: true,
						},
						"schema": {
							Type:     schema.TypeString,
							Required: true,
						},
						"warehouse": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"user": {
							Type:     schema.TypeString,
							Required: true,
						},
						"hashed_password": {
							Type:         schema.TypeString,
							Required:     true,
							Sensitive:    true,
							ValidateFunc: validateKBCEncryptedValue,
						},
					},
				},
			},
		},
	}
}

func resourceKeboolaSnowFlakeExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Google Drive Extractor in Keboola.")

	client := meta.(*KBCClient) //
	d.Partial(true)

	createdSnowflakeExtractorID, err := createSnowflakeExtractorConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetPartial("name")
	d.SetPartial("description")

	snowflakeExtractorDatabaseCredentials := d.Get("snowflake_db_parameters").(map[string]interface{})

	err = createSnowflakeExtractorDatabaseConfiguration(snowflakeExtractorDatabaseCredentials, createdSnowflakeExtractorID, client)

	if err != nil {
		return err
	}

	d.SetPartial("snowflake_db_parameters")

	d.SetId(createdSnowflakeExtractorID)

	d.Partial(false)

	return resourceKeboolaSnowFlakeExtractorRead(d, meta)

}

func createSnowflakeExtractorConfiguration(name string, description string, client *KBCClient) (createdSnowflakeExtractorID string, err error) {
	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", name)
	createExtractorForm.Add("description", description)

	createExtractorBuffer := buffer.FromForm(createExtractorForm)
	createResponse, err := client.PostToStorage("storage/components/keboola.ex-db-snowflake/configs", createExtractorBuffer)

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

func mapCredentialsToConfiguration(source map[string]interface{}) SnowflakeExtractorDatabaseParameters {
	databaseParameters := SnowflakeExtractorDatabaseParameters{}

	if val, ok := source["hostname"]; ok {
		databaseParameters.HostName = val.(string)
	}
	if val, ok := source["port"]; ok {
		databaseParameters.Port = val.(string)
	}
	if val, ok := source["database"]; ok {
		databaseParameters.Database = val.(string)
	}
	if val, ok := source["schema"]; ok {
		databaseParameters.Schema = val.(string)
	}
	if val, ok := source["warehouse"]; ok {
		databaseParameters.Warehouse = val.(string)
	}
	if val, ok := source["user"]; ok {
		databaseParameters.Username = val.(string)
	}
	if val, ok := source["hashed_password"]; ok {
		databaseParameters.EncryptedPassword = val.(string)
	}

	databaseParameters.Driver = "snowflake"

	return databaseParameters
}

func createSnowflakeExtractorDatabaseConfiguration(databaseConfiguration map[string]interface{}, createdSnowflakeID string, client *KBCClient) error {
	snowflakeExtractorConfiguration := SnowflakeExtractorConfiguration{}

	snowflakeExtractorConfiguration.Parameters.Database = mapCredentialsToConfiguration(databaseConfiguration)

	snowflakeWriterConfigurationJSON, err := json.Marshal(snowflakeExtractorConfiguration)

	if err != nil {
		return err
	}

	updateConfigurationRequestForm := url.Values{}
	updateConfigurationRequestForm.Add("configuration", string(snowflakeWriterConfigurationJSON))
	updateConfigurationRequestForm.Add("changeDescription", "Created database credentials")

	updateConfigurationRequestBuffer := buffer.FromForm(updateConfigurationRequestForm)

	updateConfigurationResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", createdSnowflakeID), updateConfigurationRequestBuffer)

	if hasErrors(err, updateConfigurationResponse) {
		return extractError(err, updateConfigurationResponse)
	}

	return nil
}

func resourceKeboolaSnowFlakeExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading SnowFlake Extractor from Keboola.")

	client := meta.(*KBCClient)
	getSnowFlakeExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getSnowFlakeExtractorResponse) {
		if getSnowFlakeExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getSnowFlakeExtractorResponse)
	}

	var snowFlakeExtractor SnowFlakeExtractor

	decoder := json.NewDecoder(getSnowFlakeExtractorResponse.Body)
	err = decoder.Decode(&snowFlakeExtractor)

	if err != nil {
		return err
	}

	d.Set("id", snowFlakeExtractor.ID)
	d.Set("name", snowFlakeExtractor.Name)
	d.Set("description", snowFlakeExtractor.Description)

	return nil
}

func resourceKeboolaSnowFlakeExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Snowflake Writer in Keboola.")

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var snowFlakeExtractor SnowFlakeExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&snowFlakeExtractor)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("changeDescription", "Updated Snowflake Writer configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)
	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return resourceKeboolaSnowFlakeExtractorRead(d, meta)
}

func resourceKeboolaSnowFlakeExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Snowflake Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
