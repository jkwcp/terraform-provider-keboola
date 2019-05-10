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

resource "keboola_awsredshift_writer" "DemoAWSRedshiftWriter" {
  name  = "Redshift Terraform"
  description = "This is an example of aws Redshift"
  redshift_wr_parameters{
    hostname = "joncluster.cs96vivrwyuv.us-west-2.redshift.amazonaws.com"
    port = "5439"
    username = "jon"
    hashed_password = "#2702Norland"
    database = "jondatabase"
    schema  = "public"
    enabled = false
    sshPort = "1"
    sshHost = "Test host For AWS Redshift"
    user = "Test user ssh"
  }
}
resource "keboola_aws_redshift_writer_table" "DemoAWSRedshiftWritertable" {
   writer_id = "${keboola_awsredshift_writer.DemoAWSRedshiftWriter.id}"
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

