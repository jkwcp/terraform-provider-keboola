package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

type SQLServerWriterTableItem struct {
	Name         string `json:"name"`
	DatabaseName string `json:"dbName"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   bool   `json:"nullable"`
	DefaultValue string `json:"default"`
}
type SQLServerWriterTable struct {
	DatabaseName string                     `json:"dbName"`
	Export       bool                       `json:"export"`
	Incremental  bool                       `json:"incremental"`
	TableID      string                     `json:"tableId"`
	PrimaryKey   []string                   `json:"primaryKey,omitempty"`
	Items        []SQLServerWriterTableItem `json:"items"`
}
type SQLServerWriter struct {
	ID            string                       `json:"id, omitempty"`
	Name          string                       `json:"name"`
	Description   string                       `json:"description"`
	Configuration SQLServerWriterConfiguration `json:"configuration"`
}

type SQLServerWriterParameters struct {
	Database SQLServerWriterDatabaseParameters `json:"db"`
	Tables   []SQLServerWriterTable            `json:"tables,omitempty"`
}
type SQLServerWriterStorageTable struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Columns     []string `json:"columns"`
}

type SQLServerWriterStorage struct {
	Input struct {
		Tables []SQLServerWriterStorageTable `json:"tables,omitempty"`
	} `json:"input,omitempty"`
}
type SQLServerWriterConfiguration struct {
	Parameters SQLServerWriterParameters `json:"parameters"`
	Storage    SQLServerWriterStorage    `json:"storage,omitempty"`
}
type SQLServerWriterDatabaseParameters struct {
	HostName          string `json:"host"`
	Database          string `json:"database"`
	Instance          string `json:"instance"`
	Password          string `json:"password,omitempty"`
	EncryptedPassword string `json:"#password,omitempty"`
	Username          string `json:"user"`
	Driver            string `json:"driver"`
	Version           string `json:"tdsVersion"`
	Port              string `json:"port"`
}

type ProvisionSQLServerResponse struct {
	Status      string `json:"status"`
	Credentials struct {
		HostName string `json:"hostname"`
		Port     int    `json:"port"`
		Instance string `json:"instance"`
		Database string `json:"db"`
		Username string `json:"user"`
		Password string `json:"password"`
		Driver   string `json:"driver"`
		Version  string `json:"tdsVersion"`
		//WorkspaceID int    `json:"workspaceId"`
	} `json:"credentials"`
}

func resourceKeboolaSQLServerWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSQLServerWriterCreate,
		Read:   resourceKeboolaSQLServerWriterRead,
		Update: resourceKeboolaSQLServerWriterUpdate,
		Delete: resourceKeboolaSQLServerWriterDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"provision_new_instance": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
				ForceNew: true,
			},
			"sqlserver_db_parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"hostname": {
							Type:     schema.TypeString,
							Required: true,
						},
						"instance": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"port": {
							Type:     schema.TypeInt,
							Optional: true,
							Default:  1433,
						},
						"username": {
							Type:     schema.TypeString,
							Required: true,
						},
						"hashed_password": {
							Type:         schema.TypeString,
							Required:     true,
							Sensitive:    true,
							ValidateFunc: validateKBCEncryptedValue,
						},
						"database": {
							Type:     schema.TypeString,
							Required: true,
						},
						"tdsVersion": {
							Type:     schema.TypeFloat,
							Required: true,
							Default:  7.4,
						},
						"ssh_tunnel": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
					},
				},
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

func resourceKeboolaSQLServerWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating SQLServer Writer in Keboola.")

	client := meta.(*KBCClient)

	d.Partial(true)
	createSQLServerID, err := createSQLServerWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetPartial("name")
	d.SetPartial("description")

	err = createSQLServerAccessToken(createSQLServerID, client)

	if err != nil {
		return err
	}

	SQLServerDatabaseCredentials := d.Get("sqlserver_db_parameters").(map[string]interface{})
	/*
		if d.Get("provision_new_instance").(bool) == true {
			provisionedSQLServer, err := provisionSQLServerInstance(client)

			if err != nil {
				return err
			}

			SQLServerDatabaseCredentials = map[string]interface{}{
				"hostname":        provisionedSQLServer.Credentials.HostName,
				"port":            strconv.Itoa(provisionedSQLServer.Credentials.Port),
				"database":        provisionedSQLServer.Credentials.Database,
				"tdsVersion":      provisionedSQLServer.Credentials.Version,
				"username":        provisionedSQLServer.Credentials.Username,
				"hashed_password": provisionedSQLServer.Credentials.Password,
				"instance":        provisionedSQLServer.Credentials.Instance,
			}

		}
	*/
	//Need to configure configuration
	err = createSQLServerCredentialsConfiguration(SQLServerDatabaseCredentials, createSQLServerID, client)

	if err != nil {
		return err
	}

	d.SetPartial("sqlserver_db_parameters")

	d.SetId(createSQLServerID)

	d.Partial(false)

	return resourceKeboolaSQLServerWriterRead(d, meta)
}

func createSQLServerAccessToken(SQLServerID string, client *KBCClient) error {
	createAccessTokenForm := url.Values{}
	createAccessTokenForm.Add("description", fmt.Sprintf("wrdbSqlServer_%s", SQLServerID))
	createAccessTokenForm.Add("canManageBuckets", "1")

	createAccessTokenBuffer := buffer.FromForm(createAccessTokenForm)

	createAccessTokenResponse, err := client.PostToStorage("storage/tokens", createAccessTokenBuffer)

	if hasErrors(err, createAccessTokenResponse) {
		return extractError(err, createAccessTokenResponse)
	}

	return nil
}
func createSQLServerWriterConfiguration(name string, description string, client *KBCClient) (createdSQLServerID string, err error) {
	createWriterForm := url.Values{}
	createWriterForm.Add("name", name)
	createWriterForm.Add("description", description)

	createWriterBuffer := buffer.FromForm(createWriterForm)

	createResponse, err := client.PostToStorage("storage/components/keboola.wr-db-mssql-v2/configs", createWriterBuffer)

	if hasErrors(err, createResponse) {
		return "", extractError(err, createResponse)
	}

	var createWriterResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createWriterResult)

	if err != nil {
		return "", err
	}

	return string(createWriterResult.ID), nil
}

/*
func provisionSQLServerInstance(client *KBCClient) (provisionedSQLServerResponse *ProvisionSQLServerResponse, err error) {
	provisionSQLServerBuffer := bytes.NewBufferString("{ \"type\": \"writer\" }")
	provisionSQLServerResponse, err := client.PostToSyrup("provisioning/snowflake", provisionSQLServerBuffer)

	if hasErrors(err, provisionSQLServerResponse) {
		return nil, extractError(err, provisionSQLServerResponse)
	}

	var provisionedSQLServer ProvisionSQLServerResponse

	provisionedSQLServerDecoder := json.NewDecoder(provisionSQLServerResponse.Body)
	err = provisionedSQLServerDecoder.Decode(&provisionedSQLServer)

	if err != nil {
		return nil, err
	}

	if provisionSQLServerResponse.StatusCode < 200 || provisionSQLServerResponse.StatusCode > 299 {
		return nil, fmt.Errorf("Unable to provision Sql Server instance (status code: %v)", provisionSQLServerResponse.StatusCode)
	}

	return &provisionedSQLServer, nil
}
*/
func mapSQLServerCredentialsToConfiguration(source map[string]interface{}) SQLServerWriterDatabaseParameters {
	databaseParameters := SQLServerWriterDatabaseParameters{}

	if val, ok := source["hostname"]; ok {
		databaseParameters.HostName = val.(string)
	}
	if val, ok := source["port"]; ok {
		databaseParameters.Port = val.(string)
	}
	if val, ok := source["database"]; ok {
		databaseParameters.Database = val.(string)
	}

	if val, ok := source["tdsVersion"]; ok {
		databaseParameters.Version = val.(string)
	}

	if val, ok := source["instance"]; ok {
		databaseParameters.Instance = val.(string)
	}
	if val, ok := source["username"]; ok {
		databaseParameters.Username = val.(string)
	}
	if val, ok := source["hashed_password"]; ok {
		databaseParameters.EncryptedPassword = val.(string)
	}
	/*
		if val, ok := source["ssh_tunnel"]; ok {
			databaseParameters.EncryptedPassword = val.(string)
		}
	*/
	databaseParameters.Driver = "mssql"

	return databaseParameters
}
func createSQLServerCredentialsConfiguration(sqlserverCredentials map[string]interface{}, createdSQLServerID string, client *KBCClient) error {

	sqlserverWriterConfiguration := SQLServerWriterConfiguration{}

	sqlserverWriterConfiguration.Parameters.Database = mapSQLServerCredentialsToConfiguration(sqlserverCredentials)

	sqlserverWriterConfigurationJSON, err := json.Marshal(sqlserverWriterConfiguration)

	if err != nil {
		return err
	}

	updateConfigurationRequestForm := url.Values{}
	updateConfigurationRequestForm.Add("configuration", string(sqlserverWriterConfigurationJSON))
	updateConfigurationRequestForm.Add("changeDescription", "Created database credentials")

	updateConfigurationRequestBuffer := buffer.FromForm(updateConfigurationRequestForm)

	updateConfigurationResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-mssql-v2/configs/%s", createdSQLServerID), updateConfigurationRequestBuffer)

	if hasErrors(err, updateConfigurationResponse) {
		return extractError(err, updateConfigurationResponse)
	}

	return nil
}
func resourceKeboolaSQLServerWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading SQLServer Writers from Keboola.")

	client := meta.(*KBCClient)
	getSQLServerWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mssql-v2/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getSQLServerWriterResponse) {
		if getSQLServerWriterResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getSQLServerWriterResponse)
	}

	var sqlserverwriter SQLServerWriter
	decoder := json.NewDecoder(getSQLServerWriterResponse.Body)
	err = decoder.Decode(&sqlserverwriter)

	if err != nil {
		return err
	}

	d.Set("id", sqlserverwriter.ID)
	d.Set("name", sqlserverwriter.Name)
	d.Set("description", sqlserverwriter.Description)

	if d.Get("provision_new_database") == false {
		dbParameters := make(map[string]interface{})

		databaseCredentials := sqlserverwriter.Configuration.Parameters.Database

		dbParameters["hostname"] = databaseCredentials.HostName
		dbParameters["port"] = databaseCredentials.Port
		dbParameters["database"] = databaseCredentials.Database
		dbParameters["tdsVersion"] = databaseCredentials.Version
		dbParameters["instance"] = databaseCredentials.Instance

		dbParameters["username"] = databaseCredentials.Username
		dbParameters["hashed_password"] = databaseCredentials.EncryptedPassword

		//dbParameters["ssh_tunnel"] = databaseCredentials.EncryptedPassword
		d.Set("sqlserver_db_parameters", dbParameters)
	}

	return nil
}
func resourceKeboolaSQLServerWriterUpdate(d *schema.ResourceData, meta interface{}) error {

	log.Println("[INFO] Updating SQLServer Writer in Keboola.")

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf(fmt.Sprintf("storage/components/keboola.wr-db-mssql-v2/configs/%s", d.Id())))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var sqlserverwriter SQLServerWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&sqlserverwriter)

	if err != nil {
		return err
	}

	sqlserverCredentials := d.Get("sqlserver_db_parameters").(map[string]interface{})

	if d.Get("provision_new_instance").(bool) == false {
		sqlserverwriter.Configuration.Parameters.Database = mapSQLServerCredentialsToConfiguration(sqlserverCredentials)
	}

	sqlserverConfigJSON, err := json.Marshal(sqlserverwriter.Configuration)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("configuration", string(sqlserverConfigJSON))
	updateCredentialsForm.Add("changeDescription", "Updated SQL Server Writer configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)

	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-mssql-v2/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return resourceKeboolaSQLServerWriterRead(d, meta)
}

func resourceKeboolaSQLServerWriterDelete(d *schema.ResourceData, meta interface{}) error {

	log.Printf("[INFO] Deleting SQL Server Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mssql-v2/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil

}
