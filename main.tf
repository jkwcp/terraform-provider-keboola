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
  api_key = "5987-152826-LjsNzSfdvOXCU9ByLvWLdF4nlZ7nqsOV4s63bFiJ"
}
/*
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
*/
resource "keboola_sqlserver_writer" "SqlServerWriter5" {
  name  = "JON"
  description = "This is an example of SQL Server From Terraform "

  sqlserver_db_parameters{
    hostname = "jondavies.database.windows.net"

    port = "1433"
    username = "Jon"
    hashed_password = "#2702norland"
    database = "JonDatabase"
    tdsVersion = "7.4"
    enabled = false
    sshPort = "1"
    sshHost = "Test host For AWS Redshift"
    user = "Test user ssh"
 
  }
}

resource "keboola_sqlserver_writer_tables" "SqlServerWriterTable4" {
   writer_id = "${keboola_sqlserver_writer.SqlServerWriter5.id}"
    table{
      db_name = "Jontest_table2"
      table_id = "out.c-jontransformation.cars_population"
      export = true
      incremental = false

   where_column= "total_cars"
   changed_since = "-15 minutes"
  where_values = [" "]
        items{
          name = "name"
          db_name = "tname"
          type = "nvarchar"
          nullable = true
          default = "jon"
          size = "255"
        }

    }
 
  
}

