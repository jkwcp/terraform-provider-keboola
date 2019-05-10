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

resource "keboola_snowflake_extractor" "DemoSnowflakeExtractor8" {
  name = "Snowflake Extractor22"
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

resource "keboola_extractor_table" "DemoExtractorTable8" {
  extractor_id = "${keboola_snowflake_extractor.DemoSnowflakeExtractor8.id}"
  table{  
    enabled=true
    name="ACCOUNT"
    output="in.c-keboola-ex-db-snowflake-501665680.cars"
    schema="HELP_TUTORIAL"
    table_name="ACCOUNT"
    table_id=42350
  }
}







resource "keboola_sqlserver_writer_tables" "SqlServerWriterTable4" {
   writer_id = "${keboola_sqlserver_writer.SqlServerWriter5.id}"
    table{
      db_name = "Jontest_table2"
      table_id = "out.c-jontransformation.cars_population"
      export = true
      incremental = false
   primary_key = ["tname"]
   where_column= "total_cars"
   changed_since = "-15 minutes"
  where_values = [" "]
        column{
          name = "name"
          db_name = "tname"
          type = "nvarchar"
          nullable = true
          default = "jon"
          size = "255"
        }

    }
     table{
      db_name = "Jontest_table3"
      table_id = "in.c-keboola-ex-db-snowflake-500829435.cars"
      export = true
      incremental = false
   primary_key = ["country"]
   where_column= "total_cars"
   changed_since = "-15 minutes"
  where_values = [" "]
        column{
          name = "country"
          db_name = "country2"
          type = "nvarchar"
          nullable = true
          default = "jon"
          size = "255"
        }

    }
 
  
}

