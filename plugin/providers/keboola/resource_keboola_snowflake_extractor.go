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

//SnowflakeExtractorParameters
//Snowflake Extractor's configuration parameters that includes database credentials and sourcing tables
//Incomplete: need to add the sourcing tables
type SnowflakeExtractorParameters struct {
	Database SnowflakeExtractorDatabaseParameters `json:"db"`
	Tables   []ExtractorTable                     `json:"tables,omitempty"`
}

//SnowflakeExtractorDatabaseParameters: configurations on sourcing database
//Complete
type SnowflakeExtractorDatabaseParameters struct {
	HostName          string `json:"host"`
	Database          string `json:"database"`
	Password          string `json:"password,omitempty"`
	EncryptedPassword string `json:"#password,omitempty"`
	User              string `json:"user"`
	Schema            string `json:"schema"`
	Port              string `json:"port"`
	Driver            string `json:"driver"`
	Warehouse         string `json:"warehouse"`
}

type SnowflakeExtractorDatabaseTable struct {
	Schema    string `json:"schema"`
	TableName string `json:"tableName"`
}

type ExtractorTable struct {
	Enabled     bool                            `json:"enabled"`
	TableID     int                             `json:"id,omitempty"`
	Name        string                          `json:"name"`
	Incremental bool                            `json:"incremental, omitempty"`
	OutputTable string                          `json:"outputTable,omitempty"` //
	PrimaryKey  []string                        `json:"primaryKey,omitempty"`
	Columns     []string                        `json:"columns,omitempty"`
	Table       SnowflakeExtractorDatabaseTable `json:"table"`
}

//SnowflakeExtractionConfiguration: component configuration
//Complete
type SnowflakeExtractorConfiguration struct {
	Parameters SnowflakeExtractorParameters `json:"parameters"`
}

//Snowflake Extractor
//Complete
type SnowFlakeExtractor struct {
	ID            string                          `json:"id"`
	Name          string                          `json:"name"`
	Description   string                          `json:"description"`
	Configuration SnowflakeExtractorConfiguration `json:"configuration"`
}

//endregion

func resourceKeboolaSnowlakeExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowflakeExtractorCreate,
		Read:   resourceKeboolaSnowflakeExtractorRead,
		Update: resourceKeboolaSnowflakeExtractorUpdate,
		Delete: resourceKeboolaSnowflakeExtractorDelete,

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

//Creates the SnowflakeExtractor on Keboola Connection
//Called when new Snowflake extractor added to the terraform
//Incomplete: needs to add the sourcing tables while create the component
func resourceKeboolaSnowflakeExtractorCreate(d *schema.ResourceData, meta interface{}) error {
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

	return resourceKeboolaSnowflakeExtractorRead(d, meta)

}

//Initial component, add component's name and description to configs and return the component's Id
//Called by resourceKeboolaSnowflakeExtractorCreate
//Complete
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

//Create database credentials
//Called by resourceKeboolaSnowflakeExtractorCreate
//Complete
func createSnowflakeExtractorDatabaseConfiguration(databaseConfiguration map[string]interface{}, createdSnowflakeID string, client *KBCClient) error {
	snowflakeExtractorConfiguration := SnowflakeExtractorConfiguration{}

	var err error
	err = nil

	snowflakeExtractorConfiguration.Parameters.Database, err = mapCredentialsToConfiguration(databaseConfiguration, client)

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

//Maps entries on terraform to database configurations
//Called by createSnowflakeExtractorDatabaseConfiguration
//Complete
func mapCredentialsToConfiguration(source map[string]interface{}, client *KBCClient) (SnowflakeExtractorDatabaseParameters, error) {
	databaseParameters := SnowflakeExtractorDatabaseParameters{}
	var err error
	err = nil

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
		databaseParameters.User = val.(string)
	}
	if val, ok := source["hashed_password"]; ok {
		databaseParameters.EncryptedPassword, err = encyrptPassword("keboola.ex-db-snowflake", val.(string), client)
	}

	databaseParameters.Driver = "snowflake"

	return databaseParameters, err
}

//Reads component configuration from Keboola Connection and compare with local Terraforms,
//updates terraform if changes have been made on Keboola Platform
//Called when update/create is executed
//Complete
func resourceKeboolaSnowflakeExtractorRead(d *schema.ResourceData, meta interface{}) error {
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

	dbParameters := make(map[string]interface{})

	databaseCredentials := snowFlakeExtractor.Configuration.Parameters.Database

	dbParameters["hostname"] = databaseCredentials.HostName
	dbParameters["port"] = databaseCredentials.Port
	dbParameters["database"] = databaseCredentials.Database
	dbParameters["schema"] = databaseCredentials.Schema
	dbParameters["warehouse"] = databaseCredentials.Warehouse
	dbParameters["user"] = databaseCredentials.User
	dbParameters["hashed_password"] = databaseCredentials.EncryptedPassword

	d.Set("snowflake_db_parameters", dbParameters)

	return nil
}

//Updates component configuration on Keboola Connection
//Called when local terraform was changed
//Complete
func resourceKeboolaSnowflakeExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Snowflake Extractor in Keboola.")

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
	updateCredentialsForm.Add("changeDescription", "Updated Snowflake Extractor configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)
	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	snowFlakeDatabaseCredentials := d.Get("snowflake_db_parameters").(map[string]interface{})
	err = createSnowflakeExtractorDatabaseConfiguration(snowFlakeDatabaseCredentials, d.Id(), client)

	return resourceKeboolaSnowflakeExtractorRead(d, meta)
}

//Destroy a Snowflake extractor
//Called when the components is removed from terraform
//Complete
func resourceKeboolaSnowflakeExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Snowflake Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
