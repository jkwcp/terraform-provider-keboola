package keboola

//This isn't complete
import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

type AWSRedshiftWriterDatabaseParameters struct {
	HostName          string `json:"host"`
	Database          string `json:"database"`
	Password          string `json:"password,omitempty"`
	EncryptedPassword string `json:"#password,omitempty"`
	Username          string `json:"user"`
	Schema            string `json:"schema"`
	Port              string `json:"port"`
	Driver            string `json:"driver"`
}

type AWSRedShiftWriterTableItem struct {
	Name         string `json:"name"`
	DatabaseName string `json:"dbName"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   bool   `json:"nullable"`
	DefaultValue string `json:"default"`
}

type AWSRedShiftWriterTable struct {
	DatabaseName string                       `json:"dbName"`
	Export       bool                         `json:"export"`
	Incremental  bool                         `json:"incremental"`
	TableID      string                       `json:"tableId"`
	PrimaryKey   []string                     `json:"primaryKey,omitempty"`
	Items        []AWSRedShiftWriterTableItem `json:"items"`
}

type AWSRedShiftWriterParameters struct {
	Database AWSRedshiftWriterDatabaseParameters `json:"db"`
	Tables   []AWSRedShiftWriterTable            `json:"tables,omitempty"`
}

type AWSRedShiftWriterStorageTable struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Columns     []string `json:"columns"`
}

type AWSRedShiftWriterStorage struct {
	Input struct {
		Tables []AWSRedShiftWriterStorageTable `json:"tables,omitempty"`
	} `json:"input,omitempty"`
}
type AWSRedShiftWriterConfiguration struct {
	Parameters AWSRedShiftWriterParameters `json:"parameters"`
	Storage    AWSRedShiftWriterStorage    `json:"storage,omitempty"`
}
type ProvisionedAWSRedShiftResponse struct {
	Status      string `json:"status"`
	Credentials struct {
		ID          int    `json:"id"`
		Hostname    string `json:"host"`
		Port        int    `json:"port"`
		Database    string `json:"db"`
		Schema      string `json:"schema"`
		Warehouse   string `json:"warehouse"`
		Username    string `json:"user"`
		Password    string `json:"password"`
		WorkspaceID int    `json:"workspaceId"`
	} `json:"credentials"`
}
type AWSRedShiftWriter struct {
	ID            string                         `json:"id,omitempty"`
	Name          string                         `json:"name"`
	Description   string                         `json:"id,description"`
	Configuration AWSRedShiftWriterConfiguration `json:"configuration"`
}

//What does it do:
// It  is the main function to the resource AWSRedShfitWriter. It sees if the sql writer needs to Update create read and delete.
// ALso it gives a map to what of what varibles are required or optional for keboola platform.
//when does it get called:
// It gets called when the keboola tf resource calls it.
//Completed:
// Yes
func resourceKeboolaAWSRedshiftWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAWSRedshiftWriterCreate,
		Read:   resourceKeboolaAWSRedShiftWriterRead,
		Update: resourceKeboolaAWSRedShiftWriterUpdate,
		Delete: resourceKeboolaAWSRedShiftWriterDelete,
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

			"redshift_wr_parameters": {
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
							Default:  5439,
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
// It creates a AWS Redshift writer component on keboola and intializing the valribles you put to the kebools script.
//When does it get called:
// It called when the terraform script has a new resource name.
//Completed:
// Yes.
func resourceKeboolaAWSRedshiftWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating AWSRedShift Writer in Keboola")
	client := meta.(*KBCClient)

	d.Partial(true)

	createdAWSRedShiftID, err := createAWSRedShiftWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)
	if err != nil {
		return err
	}

	d.SetPartial("name")
	d.SetPartial("description")

	err = createAWSRedShiftAccessToken(createdAWSRedShiftID, client)
	if err != nil {
		return err
	}
	awsredshiftDatabaseCredentials := d.Get("redshift_wr_parameters").(map[string]interface{})

	err = createRedShiftAWSCredentialsConfiguration(awsredshiftDatabaseCredentials, createdAWSRedShiftID, client)

	if err != nil {
		return err
	}

	d.SetPartial("redshift_wr_parameters")

	d.SetId(createdAWSRedShiftID)
	d.Partial(false)
	return resourceKeboolaAWSRedShiftWriterRead(d, meta)
}

func createAWSRedShiftWriterConfiguration(name string, description string, client *KBCClient) (createAWSRedShiftID string, err error) {
	createWriterForm := url.Values{}
	createWriterForm.Add("name", name)
	createWriterForm.Add("description", description)

	createWriterBuffer := buffer.FromForm(createWriterForm)

	createResponse, err := client.PostToStorage("storage/components/keboola.wr-redshift-v2/configs", createWriterBuffer)

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
// It creates an access token for your aws RedShift writer
//When does it get called:
// when you create a new terraform resource name
//Completed:
func createAWSRedShiftAccessToken(AWSRedShiftID string, client *KBCClient) error {
	createAccessTokenForm := url.Values{}
	createAccessTokenForm.Add("description", fmt.Sprintf("wrredshift_%s", AWSRedShiftID))
	createAccessTokenForm.Add("canManageBuckets", "1")

	createAccessTokenBuffer := buffer.FromForm(createAccessTokenForm)

	createAccessTokenResponse, err := client.PostToStorage("storage/tokens", createAccessTokenBuffer)

	if hasErrors(err, createAccessTokenResponse) {
		return extractError(err, createAccessTokenResponse)
	}
	return nil

}

//What does it do:
//AWS Redshift credentials to configuration for the ddatabase.  puts all the values for credentials of the database in the
//When does it get called:
// It gets called for the resource update and the creation
//Completed:
// Yes.
func mapAWSRedShiftCredentialsToConfiguration(source map[string]interface{}) AWSRedshiftWriterDatabaseParameters {
	databaseParameters := AWSRedshiftWriterDatabaseParameters{}

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

	if val, ok := source["username"]; ok {
		databaseParameters.Username = val.(string)
	}
	if val, ok := source["hashed_password"]; ok {
		databaseParameters.EncryptedPassword = val.(string)
	}

	databaseParameters.Driver = "redshift"

	return databaseParameters
}

//What does it do:
// It creates a new configruation for your AWS Redshift and add the name and description you put for that configuration
//When does it get called:
//when the create method gets called it creates a new configuratiuon
//Completed:
// Yes.
func createRedShiftAWSCredentialsConfiguration(awsredshiftCredentials map[string]interface{}, createdawsredshiftID string, client *KBCClient) error {
	awsredshiftWriterConfiguration := AWSRedShiftWriterConfiguration{}

	awsredshiftWriterConfiguration.Parameters.Database = mapAWSRedShiftCredentialsToConfiguration(awsredshiftCredentials)

	awsredshiftWriterConfigurationJSON, err := json.Marshal(awsredshiftWriterConfiguration)

	if err != nil {
		return err
	}

	updateConfigurationRequestForm := url.Values{}
	updateConfigurationRequestForm.Add("configuration", string(awsredshiftWriterConfigurationJSON))
	updateConfigurationRequestForm.Add("changeDescription", "Created database credentials")

	updateConfigurationRequestBuffer := buffer.FromForm(updateConfigurationRequestForm)

	updateConfigurationResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", createdawsredshiftID), updateConfigurationRequestBuffer)

	if hasErrors(err, updateConfigurationResponse) {
		return extractError(err, updateConfigurationResponse)
	}

	return nil
}

//What does it do:
//Aws Redshift Read allows you to see what is different from the terraform script and keboola platform and tells us if any changes where made
//When does it get called:
// It gets called for the resource update and the creation
//Completed:
// Yes.
func resourceKeboolaAWSRedShiftWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] REading AWS RedShift Writer From Keboola")
	client := meta.(*KBCClient)
	getAWSRedShiftWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}
	if hasErrors(err, getAWSRedShiftWriterResponse) {
		if getAWSRedShiftWriterResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}
		return extractError(err, getAWSRedShiftWriterResponse)
	}
	var awsredshiftwriter AWSRedShiftWriter
	decoder := json.NewDecoder(getAWSRedShiftWriterResponse.Body)
	err = decoder.Decode(&awsredshiftwriter)

	if err != nil {
		return err
	}
	d.Set("id", awsredshiftwriter.ID)
	d.Set("name", awsredshiftwriter.Name)
	d.Set("description", awsredshiftwriter.Description)

	if d.Get("provision_new_database") == false {
		dbParameters := make(map[string]interface{})

		databaseCredentials := awsredshiftwriter.Configuration.Parameters.Database

		dbParameters["hostname"] = databaseCredentials.HostName
		dbParameters["port"] = databaseCredentials.Port
		dbParameters["database"] = databaseCredentials.Database
		dbParameters["schema"] = databaseCredentials.Schema
		dbParameters["username"] = databaseCredentials.Username
		dbParameters["hashed_password"] = databaseCredentials.EncryptedPassword

		d.Set("redshift_wr_parameters", dbParameters)
	}

	return nil
}

//What does it do:
//AWS Redshift update updates the keboola platform when changes have been make.
//When does it get called:
// It  get called from the terraform script in the resources
//Completed:
// Yes.
func resourceKeboolaAWSRedShiftWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating AWS RedShift Writer in Keboola.")

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var awsredshiftWriter AWSRedShiftWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&awsredshiftWriter)

	if err != nil {
		return err
	}

	awsredshiftCredentials := d.Get("redshift_wr_parameters").(map[string]interface{})

	awsredshiftWriter.Configuration.Parameters.Database = mapAWSRedShiftCredentialsToConfiguration(awsredshiftCredentials)

	awsredshiftConfigJSON, err := json.Marshal(awsredshiftWriter.Configuration)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("configuration", string(awsredshiftConfigJSON))
	updateCredentialsForm.Add("changeDescription", "Updated Snowflake Writer configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)

	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return resourceKeboolaAWSRedShiftWriterRead(d, meta)
}

//What does it do:
//It destory the information when the terraform block is removed
//When does it get called:
// when block of the terraform script is removed
//Completed:
// Yes.
func resourceKeboolaAWSRedShiftWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting AWS Redshift Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
