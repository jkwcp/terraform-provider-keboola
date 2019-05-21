package keboola

//4900
//Completed
import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"

	"github.com/hashicorp/terraform/helper/schema"

	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

type SQLServerWriterDatabaseParameters struct {
	HostName          string    `json:"host"`
	Database          string    `json:"database"`
	Instance          string    `json:"instance"`
	EncryptedPassword string    `json:"#password"`
	Username          string    `json:"user"`
	Driver            string    `json:"driver"`
	Version           string    `json:"tdsVersion"`
	Port              string    `json:"port"`
	SSH               SSHTunnel `json:"ssh"`
}

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

type SQLServerWriterParameters struct {
	Database SQLServerWriterDatabaseParameters `json:"db"`
	Tables   []SQLServerWriterTable            `json:"tables,omitempty"`
}
type SQLServerWriterStorageTable struct {
	Source        string   `json:"source"`
	Destination   string   `json:"destination"`
	Columns       []string `json:"columns"`
	ChangedSince  string   `json:"changed_since,omitempty"`
	WhereColumn   string   `json:"where_column,omitempty"`
	WhereOperator string   `json:"where_operator,omitempty"`
	WhereValues   []string `json:"where_values,omitempty"`
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

type SQLServerWriter struct {
	ID            string                       `json:"id, omitempty"`
	Name          string                       `json:"name"`
	Description   string                       `json:"description"`
	Configuration SQLServerWriterConfiguration `json:"configuration"`
}

//What does it do:
// It  is the main function to the resource sql writer. It sees if the sql writer needs to Update create read and delete.
// ALso it gives a map to what of what varibles are required or optional for keboola platform.
//when does it get called:
// It gets called when the keboola provider calls it.
//Completed:
// Yes
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
			"description": {
				Type:     schema.TypeString,
				Optional: true,
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
						////////For SSH Tunnel///////
						"enabled": {
							Type:     schema.TypeBool,
							Optional: true,
						},
						"sshHost": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"user": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"sshPort": {
							Type:     schema.TypeString,
							Optional: true,
						},
						////////////////////////////////

					},
				},
			},
		},
	}
}

//What does it do:
// It creates a Sql Server writer component on keboola and intializing the valribles you put to the kebools script.
//When does it get called:
// It gets called when the resourceKeboolaSQLServerWriter calls it
//Completed:
// Yes.
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
	sqlserverDatabaseCredentials := d.Get("sqlserver_db_parameters").(map[string]interface{})

	err = createSQLServerCredentialsConfiguration(sqlserverDatabaseCredentials, createSQLServerID, client)

	if err != nil {
		return err
	}

	d.SetPartial("sqlserver_db_parameters")
	d.SetId(createSQLServerID)

	d.Partial(false)

	return resourceKeboolaSQLServerWriterRead(d, meta)
}

//What does it do:
// It creates a new configruation for your Sql sever and add the name and description you put for that configuration
//When does it get called:
//when the resourceKeboolaSQLServerWriterCreate func calls it
//Completed:
// Yes.
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

//What does it do:
// It creates an access token for your sql server writer on the keboola platform
//When does it get called:
// when the resourceKeboolaSQLServerWriterCreate func calls it
//Completed:
// Yes.
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

//What does it do:
// It creates a new configruation for your Sql sever and add the name and description you put for that terraform script
//When does it get called:
//when the resourceKeboolaSQLServerWriterCreate func calls it
//Completed:
// Yes.
func createSQLServerCredentialsConfiguration(sqlserverCredentials map[string]interface{}, createdSQLServerID string, client *KBCClient) error {

	sqlserverWriterConfiguration := SQLServerWriterConfiguration{}
	var err error
	sqlserverWriterConfiguration.Parameters.Database, err = mapSQLServerCredentialsToConfigurationDatabase(sqlserverCredentials, client)

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

//What does it do:
//Sql server credentials to configuration for the ddatabase.  puts all the values for credentials of the database in the
//When does it get called:
// It gets called for the resource resourceKeboolaSQLServerWriterCreate and the resourceKeboolaSQLServerWriterUpdate
//Completed:
// Yes.
func mapSQLServerCredentialsToConfigurationDatabase(source map[string]interface{}, client *KBCClient) (SQLServerWriterDatabaseParameters, error) {
	databaseParameters := SQLServerWriterDatabaseParameters{}
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
		databaseParameters.EncryptedPassword, err = encyrptPassword("keboola.wr-db-mssql-v2", val.(string), client)
	}
	if val, ok := source["enabled"]; ok {
		databaseParameters.SSH.Enabled, err = strconv.ParseBool(val.(string))
	}

	if val, ok := source["sshHost"]; ok {
		databaseParameters.SSH.SSHHost = val.(string)
		databaseParameters.Driver = "mssql"
		databaseParameters.SSH.SSHKey, err = client.PostToDockerCreateSSH()
		databaseParameters.SSH.SSHKey.PrivateKeyEncrypted, err = encyrptPassword("keboola.wr-db-mssql-v2", databaseParameters.SSH.SSHKey.PrivateKeyEncrypted, client)
		databaseParameters.SSH.SSHKey.PrivateKey = ""
	}
	if val, ok := source["user"]; ok {
		databaseParameters.SSH.User = val.(string)
	}
	if val, ok := source["sshPort"]; ok {
		databaseParameters.SSH.SSHPort = val.(string)
	}

	return databaseParameters, err
}

//What does it do:
//Sql server Read allows you to see what is different from the terraform script and keboola platform and tells us if any changes where made
//When does it get called:
// It gets called for the resource resourceKeboolaSQLServerWriterCreate  and the resourceKeboolaSQLServerWriterupdated call it
//Completed:
// Yes.
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

	return nil
}

//What does it do:
//Sql server update updates the keboola platform when changes have been make.
//When does it get called:
// It  get called from the resourceKeboolaSQLServerWriter
//Completed:
// Yes.
func resourceKeboolaSQLServerWriterUpdate(d *schema.ResourceData, meta interface{}) error {

	log.Println("[INFO] Updating SQLServer Writer in Keboola.")

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mssql-v2/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var sqlserverwriter SQLServerWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&sqlserverwriter)

	if err != nil {
		return err
	}

	sqlserverwriterCredentials := d.Get("sqlserver_db_parameters").(map[string]interface{})

	sqlserverwriter.Configuration.Parameters.Database, err = mapSQLServerCredentialsToConfigurationDatabase(sqlserverwriterCredentials, client)

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

//What does it do:
//It destory the information when the resourceKeboolaSQLServerWriterDelete
// When does it get called:
// when block of the terraform script is removed
//Completed:
// Yes.
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
