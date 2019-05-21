package keboola

import (
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

// Provider returns a terraform.ResourceProvider for the Keboola provider.
func Provider() terraform.ResourceProvider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"api_key": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("STORAGE_API_KEY", nil),
			},
		},

		ResourcesMap: map[string]*schema.Resource{
			"keboola_storage_table":               resourceKeboolaStorageTable(),
			"keboola_storage_bucket":              resourceKeboolaStorageBucket(),
			"keboola_transformation":              resourceKeboolaTransformation(),
			"keboola_transformation_bucket":       resourceKeboolaTransformationBucket(),
			"keboola_access_token":                resourceKeboolaAccessToken(),
			"keboola_orchestration":               resourceKeboolaOrchestration(),
			"keboola_orchestration_tasks":         resourceKeboolaOrchestrationTasks(),
			"keboola_aws_redshift_writer":         resourceKeboolaAWSRedshiftWriter(),
			"keboola_aws_redshift_writer_table":   resourceKeboolaAWSRedShiftWriterTable(),
			"keboola_sqlserver_writer":            resourceKeboolaSQLServerWriter(),
			"keboola_sqlserver_writer_tables":     resourceKeboolaSQLServerWriterTables(),
			"keboola_googledrive_extractor":       resourceGoogleDriveExtractor(),
			"keboola_awsredshift_writer":          resourceKeboolaAWSRedshiftWriter(),
			"keboola_snowflake_extractor":         resourceKeboolaSnowlakeExtractor(),
			"keboola_tableau_writer":              resourceKeboolaTableauWriter(),
			"keboola_dropbox_writer":              resourceKeboolaDropboxWriter(),
			"keboola_aws_s3bucket_writer":         resourceKeboolaAWSs3Writer(),
			"keboola_aws_s3Bucket_table":          resourceKeboolaAWSS3Bucket(),
			"keboola_snowflake_writer":            resourceKeboolaSnowflakeWriter(),
			"keboola_mysql_writer":                resourceKeboolaMySqlWriter(),
			"keboola_mysql_writer_table":          resourceKeboolaMySqlWriterTable(),
			"keboola_extractor_table":             resourceKeboolaExtractorTable(),
			"keboola_aws_s3bucket_extractor":      resourceKeboolaAWSS3Extractor(),
			"keboola_aws_s3bucket_extractor_rows": resourceKeboolaAWSS3ExtractorRows(),
			"keboola_email_attachment_extractor":  resourceKeboolaEmailAttachmentExtractor(),
		},

		ConfigureFunc: providerConfigure,
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	log.Println("[INFO] Initializing Keboola REST client")
	client := &KBCClient{
		APIKey: d.Get("api_key").(string),
	}
	return client, nil
}
