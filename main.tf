#add your own configurations

#must include api_key, look like this:

#provider "keboola" {
#  api_key = "5959-152644-8hUPlB5CRGTOLmuYWKToMQ4CWrHUcfRoogM1gkJe"
#}

#Configurations will be like:

#resource "keboola_component_writer" "Component Name" {
#     Configurations....  
#}

provider "keboola" {
  api_key = "5959-152644-jtfLgkQuUNaeYp1alpMt9GfDsjxb59FsUkQp6Dw4"
}


resource "keboola_snowflake_extractor" "DemoSnowflakeExtractor" {
  name = "Snowflake Extractor"
  description = "Snowflake Extractor"
  snowflake_db_parameters{
    hostname="kebooladev.snowflakecomputing.com"
    port="443"
    database="HELP_TUTORIAL"
    schema="HELP_TUTORIAL"
    warehouse="DEV"
    user="HELP_TUTORIAL"
    hashed_password="HELP_TUTORIAL"
  }
}

resource "keboola_googledrive_extractor" "DemoGoogleDriveExtractor" {
  name  = "Google Drive Extractor"
  description = "Google Drive Extractor"
}

resource "keboola_transformation_bucket" "DemoTransformationBucket" {
  name = "Transformation Bucket"
  description = "Transformation Bucket"

}

resource "keboola_transformation" "DemoTransformation" {
  bucket_id = "${keboola_transformation_bucket.DemoTransformationBucket.id}"
  name = "Transformation"
  description = "Transformation"
  backend = "snowflake"
  type = "simple"

}

resource "keboola_awsredshift_writer" "DemoAWSRedshiftWriter" {
  name  = "Redshift"
  description = "This is an example of aws Redshift"
  redshift_wr_parameters{
    hostname = "jondw.cs96vivrwyuv.us-west-2.redshift.amazonaws.com"
    port = "5439"
    username = "jon"
    hashed_password = "Computer#18"
    database = "jontestdb"
    schema  = "information_schema"
  }
}

resource "keboola_aws_s3bucket_writer" "DemoS3Bucket" {
  name  = "S3 Bucket"
  description = "This is an example of the S3 Bucket"
  s3_wr_parameters {
    accessKeyId = "arn:aws:s3:::a01011881-lab-3"
    bucket = "a01011881-lab-3"
    secretaccesskey = "=+hY8qbR78iXlxBKA6FzBx0rWrdfeMPbdtqHdTsJR"
  }
}

resource "keboola_dropbox_writer" "DemoDropBoxWriter" {
  name  = "Drop Box"
  description = "Demo_Dropbox"
}

resource "keboola_tableau_writer" "DemoTableauWriter" {
  name  = "Tableau"
  description = "Demo_Tableau"
}

resource "keboola_sqlserver_writer" "TermProject2" {
  name  = "Sql Server Writer"
  description = "This is an example of SQL Server "
  
  sqlserver_db_parameters{
    hostname = "jontestdb.database.windows.net"
    port = "1433"
    username = "ADP"
    hashed_password = "#2702norland"
    database = "jondbtest"
    tdsVersion = "7.4"
  }
}





