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
  api_key = "6152-156159-gLKKonW3ioH3fqDivcbXfwXb7AjI33cOdI4Ymdsb"
}

resource "keboola_snowflake_extractor" "Demo_Snowflake_Extractor6"{
  name="Demo_Snowflake_Extractor7"
  description="This is a demo"
  snowflake_db_parameters{
    hostname="kebooladev.snowflakecomputing.com"
    port=443
    database="HELP_TUTORIAL"
    schema="HELP_TUTORIAL"
    database="HELP_TUTORIAL"
    warehouse="DEV"
    user="HELP_TUTORIAL"
    hashed_password="HELP_TUTORIAL"
  }
}

resource "keboola_extractor_table" "Demo_Snowflake_Extractor_Tables6" {
  extractor_id="${keboola_snowflake_extractor.Demo_Snowflake_Extractor6.id}"
  table{
        table_id=5217
        name="cars"
        incremental=false
        table_name="cars"
        schema="HELP_TUTORIAL"
  }
  table{
        table_id=5218
        name="countries"
        incremental=false
        table_name="countries"
        schema="HELP_TUTORIAL"
  }
}
