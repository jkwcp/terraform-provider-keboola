package keboola

// this is COmplete
import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

type AWSs3WriterDatabaseParameters struct {
	AccessKeyId string `json:"accessKeyId"`
	SecretKey   string `json:"#secretAccessKey"`
	Bucket      string `json:"bucket"`
}

type AWSs3WriterDatabaseRowParameters struct {
	Prefix string `json:"prefix"`
}

type AWSs3WriterStorageTable struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

type Awss3Definition struct {
	Component string `json:"component"`
}
type Awss3parameters struct {
	Direction string `json:"direction"`
}

type AWSS3WriterTable struct {
	TableID string `json:"tableId"`
}
type AWSs3WriterStorage struct {
	Input struct {
		Tables []AWSs3WriterStorageTable `json:"tables,omitempty"`
	} `json:"input,omitempty"`
}
type Awss3WriterStorageTableBefore struct {
	ComponentStorage Awss3Definition `json:"definition"`
	DirectionStorage Awss3parameters `json:"parameters"`
}
type AWSs3WriterProcessor struct {
	Before []Awss3WriterStorageTableBefore `json:"before,omitempty"`
}

type Rowsinfo struct {
	Storage    AWSs3WriterStorage               `json:"storage,omitempty"`
	Processor  AWSs3WriterProcessor             `json:"processors,omitempty"`
	Parameters AWSs3WriterDatabaseRowParameters `json:"parameters"`
	id         string                           `json:"id"`
	name       string                           `json:"name"`
}
type AWSs3WriterConfigurationParameters struct {
	Parameters AWSs3WriterDatabaseParameters `json:"parameters"`
}
type AWSs3WriterConfigurationRows struct {
	RowsInfo Rowsinfo `json:"rows,omitempty"`
}
type AWSs3Writer struct {
	ID                                 string                             `json:"id,omitempty"`
	Name                               string                             `json:"name"`
	Description                        string                             `json:"description"`
	AWSs3WriterConfigurationParameters AWSs3WriterConfigurationParameters `json:"configuration"`
	ConfigurationRow                   AWSs3WriterConfigurationRows       `json:"configuration"`
}

//What does it do:
// It  is the main function to the resource AWS S3 Bucket. It sees if the S3 Bucket writer needs to Update, create or delete a componeent.
// ALso it gives a map to what of what varibles are required or optional for keboola platform.
//when does it get called:
// It gets called when the keboola Provider calls it.
//Completed:
// Yes
func resourceKeboolaAWSs3Writer() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAWSs3WriterCreate,
		Read:   resourceKeboolaAWSs3WriterRead,
		Update: resourceKeboolaAWSs3WriterUpdate,
		Delete: resourceKeboolaAWSs3WriterDelete,
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
			"s3_wr_parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"bucket": {
							Type:     schema.TypeString,
							Required: true,
						},
						"accesskeyid": {
							Type:     schema.TypeString,
							Required: true,
						},
						"secretaccesskey": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
		},
	}
}

//What does it do:
// It creates a AWS s3 writer component on keboola when the provider sees that this is a new component from the terraform script
//When does it get called:
// It called by resourceKeboolaAWSs3Writer
//Completed:
// Yes.
func resourceKeboolaAWSs3WriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating AWSs3 Writer in Keboola")
	client := meta.(*KBCClient)

	d.Partial(true)

	createdAWSs3ID, err := createAWSs3WriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)
	if err != nil {
		return err
	}

	d.SetPartial("name")
	d.SetPartial("description")
	err = createAWSs3AccessToken(createdAWSs3ID, client)
	if err != nil {
		return err
	}
	awss3DatabaseCredentials := d.Get("s3_wr_parameters").(map[string]interface{})

	err = creates3AWSCredentialsConfiguration(awss3DatabaseCredentials, createdAWSs3ID, client)

	if err != nil {
		return err
	}

	d.SetId(createdAWSs3ID)
	d.Partial(false)
	return resourceKeboolaAWSs3WriterRead(d, meta)
}

//What does it do:
//Creates configuration and credentials for the S3 bucket
//When does it get called:
// It called by resourceKeboolaAWSs3WriterCreate
//Completed:
// Yes.
func createAWSs3WriterConfiguration(name string, description string, client *KBCClient) (createAWSs3ID string, err error) {
	createWriterForm := url.Values{}
	createWriterForm.Add("name", name)
	createWriterForm.Add("description", description)

	createWriterBuffer := buffer.FromForm(createWriterForm)

	createResponse, err := client.PostToStorage("storage/components/keboola.wr-aws-s3/configs", createWriterBuffer)

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
// It creates an access token for your aws s3 writer
//When does it get called:
// when resourceKeboolaAWSs3WriterCreate is called
//Completed:
//Yes
func createAWSs3AccessToken(AWSs3ID string, client *KBCClient) error {
	createAccessTokenForm := url.Values{}
	createAccessTokenForm.Add("description", fmt.Sprintf("wrs3_%s", AWSs3ID))
	createAccessTokenForm.Add("canManageBuckets", "1")

	createAccessTokenBuffer := buffer.FromForm(createAccessTokenForm)

	createAccessTokenResponse, err := client.PostToStorage("storage/tokens", createAccessTokenBuffer)

	if hasErrors(err, createAccessTokenResponse) {
		return extractError(err, createAccessTokenResponse)
	}
	return nil

}

//What does it do:
//AWS s3 credentials to configuration for the database. It puts all the values for credentials of the database in the apporiate structure
//When does it get called:
// It gets called for the  resourceKeboolaAWSs3WriterUpdate and the resourceKeboolaAWSs3WriterCreate func
//Completed:
// Yes.
func mapAWSs3CredentialsToConfiguration(source map[string]interface{}, client *KBCClient) (AWSs3WriterDatabaseParameters, error) {
	Parameters := AWSs3WriterDatabaseParameters{}
	var err error
	err = nil
	if val, ok := source["bucket"]; ok {
		Parameters.Bucket = val.(string)
	}
	if val, ok := source["accesskeyId"]; ok {
		Parameters.AccessKeyId = val.(string)
	}
	if val, ok := source["secretaccesskey"]; ok {

		Parameters.SecretKey, err = encyrptPassword("keboola.wr-aws-s3", val.(string), client)
	}

	return Parameters, err
}

//What does it do:
// It creates a new configruation for your AWS s3 and add the name and description you put for that configuration
//When does it get called:
//when the resourceKeboolaAWSs3WriterCreate calls it
//Completed:
// Yes.
func creates3AWSCredentialsConfiguration(awss3Credentials map[string]interface{}, createdawss3ID string, client *KBCClient) error {
	awss3WriterConfiguration := AWSs3WriterConfigurationParameters{}
	var err error
	err = nil
	awss3WriterConfiguration.Parameters, err = mapAWSs3CredentialsToConfiguration(awss3Credentials, client)

	awss3WriterConfigurationJSON, err := json.Marshal(awss3WriterConfiguration)

	if err != nil {
		return err
	}

	updateConfigurationRequestForm := url.Values{}
	updateConfigurationRequestForm.Add("configuration", string(awss3WriterConfigurationJSON))
	updateConfigurationRequestForm.Add("changeDescription", "Created database credentials")

	updateConfigurationRequestBuffer := buffer.FromForm(updateConfigurationRequestForm)

	updateConfigurationResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", createdawss3ID), updateConfigurationRequestBuffer)

	if hasErrors(err, updateConfigurationResponse) {
		return extractError(err, updateConfigurationResponse)
	}

	return nil
}

//What does it do:
//Aws s3 Read allows you to see what is different from the terraform script and keboola platform and tells us if any changes where made
//When does it get called:
// It gets called for the resource resourceKeboolaAWSs3WriterUpdate and the resourceKeboolaAWSs3WriterCreate
//Completed:
// Yes.
func resourceKeboolaAWSs3WriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] REading AWS s3 Writer From Keboola")
	client := meta.(*KBCClient)
	getAWSs3WriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}
	if hasErrors(err, getAWSs3WriterResponse) {
		if getAWSs3WriterResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}
		return extractError(err, getAWSs3WriterResponse)
	}
	var awss3writer AWSs3Writer
	decoder := json.NewDecoder(getAWSs3WriterResponse.Body)
	err = decoder.Decode(&awss3writer)

	if err != nil {
		return err
	}
	d.Set("id", awss3writer.ID)
	d.Set("name", awss3writer.Name)
	d.Set("description", awss3writer.Description)
	if d.Get("provision_new_database") == false {
		dbParameters := make(map[string]interface{})

		databaseCredentials := awss3writer.AWSs3WriterConfigurationParameters.Parameters

		dbParameters["accesskeyid"] = databaseCredentials.AccessKeyId
		dbParameters["#secretAccessKey"] = databaseCredentials.SecretKey
		dbParameters["bucket"] = databaseCredentials.Bucket

		d.Set("s3_wr_parameters", dbParameters)
	}
	return nil
}

//What does it do:
//AWS s3 updates the keboola platform when changes have been made on the terraform script.
//When does it get called:
// It  get called by resourceKeboolaAWSs3Writer
//Completed:
// Yes.
func resourceKeboolaAWSs3WriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating AWS s3 Writer in Keboola.")

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var awss3Writer AWSs3Writer

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&awss3Writer)

	if err != nil {
		return err
	}

	awss3Credentials := d.Get("s3_wr_parameters").(map[string]interface{})

	awss3Writer.AWSs3WriterConfigurationParameters.Parameters, err = mapAWSs3CredentialsToConfiguration(awss3Credentials, client)

	awss3ConfigJSON, err := json.Marshal(awss3Writer.AWSs3WriterConfigurationParameters)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("configuration", string(awss3ConfigJSON))
	updateCredentialsForm.Add("changeDescription", "Updated Snowflake Writer configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)

	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return resourceKeboolaAWSs3WriterRead(d, meta)
}

//What does it do:
//It destory the information when the terraform block is removed
//When does it get called:
// it gets called by resourceKeboolaAWSs3Writer
//Completed:
// Yes
func resourceKeboolaAWSs3WriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting AWS s3 Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-aws-s3/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
