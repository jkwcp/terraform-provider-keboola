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

type MySqlWriterDatabaseParameters struct {
	HostName          string `json:"host"`
	Database          string `json:"database"`
	Password          string `json:"password,omitempty"`
	EncryptedPassword string `json:"#password,omitempty"`
	Username          string `json:"user"`

	Port   string    `json:"port"`
	Driver string    `json:"driver"`
	SSH    SSHTunnel `json:"ssh"`
}
type MySqlWriterParameters struct {
	Database MySqlWriterDatabaseParameters `json:"db"`
	Tables   []MySqlWriterTable            `json:"tables,omitempty"`
}
type MySqlWriterTableItem struct {
	Name         string `json:"name"`
	DatabaseName string `json:"dbName"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   bool   `json:"nullable"`
	DefaultValue string `json:"default"`
}
type MySqlWriterTable struct {
	DatabaseName string                 `json:"dbName"`
	Export       bool                   `json:"export"`
	Incremental  bool                   `json:"incremental"`
	TableID      string                 `json:"tableId"`
	PrimaryKey   []string               `json:"primaryKey,omitempty"`
	Items        []MySqlWriterTableItem `json:"items"`
}
type MySqlWriterStorageTable struct {
	Source        string   `json:"source"`
	Destination   string   `json:"destination"`
	Columns       []string `json:"columns"`
	ChangedSince  string   `json:"changed_since,omitempty"`
	WhereColumn   string   `json:"where_column,omitempty"`
	WhereOperator string   `json:"where_operator,omitempty"`
	WhereValues   []string `json:"where_values,omitempty"`
}
type MySqlWriterStorage struct {
	Input struct {
		Tables []MySqlWriterStorageTable `json:"tables,omitempty"`
	} `json:"input,omitempty"`
}

type MySqlWriter struct {
	ID            string                   `json:"id,omitempty"`
	Name          string                   `json:"name"`
	Description   string                   `json:"description"`
	Configuration MySqlWriterConfiguration `json:"configuration"`
}

type MySqlWriterConfiguration struct {
	Parameters MySqlWriterParameters `json:"parameters"`
	Storage    MySqlWriterStorage    `json:"storage,omitempty"`
}

//What does it do:
// It  is the main function to the resource MySqlWriter. It sees if the AWS Redshift writer needs to Update create read and delete.
// ALso it gives a map to what of what varibles are required or optional for keboola platform.
//when does it get called:
// It gets called when the provider calls it.
//Completed:
// Yes
func resourceKeboolaMySqlWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaMySqltWriterCreate,
		Read:   resourceKeboolaMySqlWriterRead,
		Update: resourceKeboolaMySqlWriterUpdate,
		Delete: resourceKeboolaMySqlWriterDelete,
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

			"mysql_wr_parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"host": {
							Type:     schema.TypeString,
							Required: true,
						},
						"port": {
							Type:     schema.TypeInt,
							Optional: true,
							Default:  3306,
						},
						"database": {
							Type:     schema.TypeString,
							Required: true,
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

						////////////////////SSH////////////////////
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

//What does it do:
// It creates a MySql  writer component on keboola and intializing the valribles you put to the kebools terraform script.
//When does it get called:
// It called when resourceKeboolaMySql func calls it
//Completed:
// Yes.
func resourceKeboolaMySqltWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating MySql Writer in Keboola")
	client := meta.(*KBCClient)

	d.Partial(true)

	createdMySqlID, err := createMySqlWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)
	if err != nil {
		return err
	}

	err = createMySqlAccessToken(createdMySqlID, client)
	if err != nil {
		return err
	}
	mysqlDatabaseCredentials := d.Get("mysql_wr_parameters").(map[string]interface{})
	mysqlSSHDatabaseCredentials := d.Get("mysql_wr_parameters").(map[string]interface{})
	err = createMySqlcredentialsConfiguration(mysqlDatabaseCredentials, mysqlSSHDatabaseCredentials, createdMySqlID, client)

	if err != nil {
		return err
	}

	d.SetPartial("mysql_wr_parameters")

	d.SetId(createdMySqlID)
	d.Partial(false)
	return resourceKeboolaMySqlWriterRead(d, meta)
}

//What does it do:
// It Configures the component and post it to the API Storage
//When does it get called:
// It called when the resourceKeboolaMySqlCreate function  calls it
//Completed:
// Yes.
func createMySqlcredentialsConfiguration(MySqlCredentials map[string]interface{}, sshCredentials map[string]interface{}, createdMySqlID string, client *KBCClient) error {
	var err error

	mySqlWriterConfiguration := MySqlWriterConfiguration{}

	mySqlWriterConfiguration.Parameters.Database, err = mapMySqlDatabaseCredentialsToConfiguration(MySqlCredentials, client)
	mySqlWriterConfiguration.Parameters.Database.SSH, err = mapMySqlSSHCredentialsToConfiguration(sshCredentials, client)
	mySqlWriterConfigurationJSON, err := json.Marshal(mySqlWriterConfiguration)
	if err != nil {
		return err
	}

	updateConfigurationRequestForm := url.Values{}
	updateConfigurationRequestForm.Add("configuration", string(mySqlWriterConfigurationJSON))
	updateConfigurationRequestForm.Add("changeDescription", "Created database credentials")

	updateConfigurationRequestBuffer := buffer.FromForm(updateConfigurationRequestForm)

	updateConfigurationResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", createdMySqlID), updateConfigurationRequestBuffer)

	if hasErrors(err, updateConfigurationResponse) {
		return extractError(err, updateConfigurationResponse)
	}

	return nil
}

//What does it do:
// It creates an access token for your MySql writer to use it
//When does it get called:
// when you create you call the resourceKeboolaMySqlCreate function
//Completed:
//Yes
func createMySqlAccessToken(MySqlID string, client *KBCClient) error {
	createAccessTokenForm := url.Values{}
	createAccessTokenForm.Add("description", fmt.Sprintf("wrmysql_%s", MySqlID))
	createAccessTokenForm.Add("canManageBuckets", "1")

	createAccessTokenBuffer := buffer.FromForm(createAccessTokenForm)

	createAccessTokenResponse, err := client.PostToStorage("storage/tokens", createAccessTokenBuffer)

	if hasErrors(err, createAccessTokenResponse) {
		return extractError(err, createAccessTokenResponse)
	}
	return nil

}

//What does it do:
// It Configures the component and post it to the API Storage
//When does it get called:
// It called when the resourceKeboolaMySqlWriterCreate function  calls it
//Completed:
// Yes.

func createMySqlWriterConfiguration(name string, description string, client *KBCClient) (createAWSRedShiftID string, err error) {
	createWriterForm := url.Values{}
	createWriterForm.Add("name", name)
	createWriterForm.Add("description", description)

	createWriterBuffer := buffer.FromForm(createWriterForm)

	createResponse, err := client.PostToStorage("storage/components/keboola.wr-db-mysql/configs", createWriterBuffer)

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
//MySql credentials to configuration for the ddatabase.  puts all the values for credentials of the database in the database paramter structure
//When does it get called:
// It gets called for the resource createMySqlCredentialsConfiguration and the resourceKeboolaMySqlWriterCreate func.
//Completed:
// Yes.
func mapMySqlDatabaseCredentialsToConfiguration(source map[string]interface{}, client *KBCClient) (MySqlWriterDatabaseParameters, error) {
	databaseParameters := MySqlWriterDatabaseParameters{}
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

	if val, ok := source["username"]; ok {
		databaseParameters.Username = val.(string)
	}
	if val, ok := source["hashed_password"]; ok {
		databaseParameters.EncryptedPassword, err = encyrptPassword("keboola.wr-db-mysql", val.(string), client)
	}

	return databaseParameters, err
}

//What does it do:
//MySql SSH  credentials to configuration for the ddatabase.  puts all the values for credentials of the SSh in the SSH paramter structure
//When does it get called:
// It gets called for the resource createMySQlCredentialsConfiguration and the resourceKeboolaMySqlWriterCreate func.
//Completed:
// Yes.
func mapMySqlSSHCredentialsToConfiguration(source map[string]interface{}, client *KBCClient) (SSHTunnel, error) {

	sshParameters := SSHTunnel{}
	var err error
	err = nil
	if val, ok := source["enabled"]; ok {

		sshParameters.Enabled, err = strconv.ParseBool(val.(string))
		sshParameters.SSHKey, err = client.PostToDockerCreateSSH()
		sshParameters.SSHKey.PrivateKeyEncrypted, err = encyrptPassword("keboola.wr-db-mysql", sshParameters.SSHKey.PrivateKeyEncrypted, client)
		sshParameters.SSHKey.PrivateKey = ""
	}
	if val, ok := source["sshHost"]; ok {
		sshParameters.SSHHost = val.(string)
	}
	if val, ok := source["user"]; ok {
		sshParameters.User = val.(string)
	}
	if val, ok := source["sshPort"]; ok {
		sshParameters.SSHPort = val.(string)
	}

	return sshParameters, err
}

//What does it do:
//MySql Read allows you to see what is different from the terraform script and keboola platform and tells us if any changes where made
//When does it get called:
// It gets called for the resource resourceKeboolaMySqlWriterUpdate and the resourceKeboolaMySqlWriterCreate
//Completed:
// Yes.
func resourceKeboolaMySqlWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading MySql Writer From Keboola")
	client := meta.(*KBCClient)
	getMySqlWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}
	if hasErrors(err, getMySqlWriterResponse) {
		if getMySqlWriterResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}
		return extractError(err, getMySqlWriterResponse)
	}
	var mysqlwriter MySqlWriter
	decoder := json.NewDecoder(getMySqlWriterResponse.Body)
	err = decoder.Decode(&mysqlwriter)

	if err != nil {
		return err
	}
	d.Set("id", mysqlwriter.ID)
	d.Set("name", mysqlwriter.Name)
	d.Set("description", mysqlwriter.Description)

	if d.Get("provision_new_database") == false {
		dbParameters := make(map[string]interface{})
		sshParameters := make(map[string]interface{})
		databaseCredentials := mysqlwriter.Configuration.Parameters.Database
		sshCredentials := mysqlwriter.Configuration.Parameters.Database.SSH

		dbParameters["hostname"] = databaseCredentials.HostName
		dbParameters["port"] = databaseCredentials.Port
		dbParameters["database"] = databaseCredentials.Database

		dbParameters["username"] = databaseCredentials.Username
		dbParameters["hashed_password"] = databaseCredentials.EncryptedPassword

		sshParameters["enabled"] = sshCredentials.Enabled
		sshParameters["sshHost"] = sshCredentials.SSHHost

		sshParameters["user"] = sshCredentials.User
		sshParameters["sshPort"] = sshCredentials.SSHPort

		d.Set("mysql_wr_parameters", dbParameters)
		d.Set("mysql_wr_parameters", sshParameters)
	}

	return nil
}

//What does it do:
//MySql update updates the keboola platform when changes have been make.
//When does it get called:
// It  get called from the resourceKeboolaMySqlWriter func.
//Completed:
// Yes.
func resourceKeboolaMySqlWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating MySql Writer in Keboola.")

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var mysqlwriter MySqlWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&mysqlwriter)

	if err != nil {
		return err
	}
	mysqlCredentials := d.Get("mysql_wr_parameters").(map[string]interface{})
	mysshCredentials := d.Get("mysql_wr_parameters").(map[string]interface{})
	mysqlwriter.Configuration.Parameters.Database, err = mapMySqlDatabaseCredentialsToConfiguration(mysqlCredentials, client)
	mysqlwriter.Configuration.Parameters.Database.SSH, err = mapMySqlSSHCredentialsToConfiguration(mysshCredentials, client)
	mysqlwriterConfigJSON, err := json.Marshal(mysqlwriter.Configuration)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("configuration", string(mysqlwriterConfigJSON))
	updateCredentialsForm.Add("changeDescription", "Updated MySql Writer configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)

	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return resourceKeboolaMySqlWriterRead(d, meta)

}

//What does it do:
//It destory the information when the terraform block is removed
//When does it get called:
// when block of the terraform script is removed it gets called by resourceKeboolaMySqlWriter
//Completed:
// Yes.
func resourceKeboolaMySqlWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting mySql Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
