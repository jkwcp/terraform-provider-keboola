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
resource "keboola_mysql_writer" "MySqlWriterTest" {
name = "MySql"
description = "Description test"

mysql_wr_parameters{
   hostname = "jontestmysql.mysql.database.azure.com"
    port = "3306"
    username = "jon@jontestmysql"
    hashed_password = "#2702norland"
    database = "writer_sample"
    enabled = false
    sshPort = "11"
    sshHost = "MySql Hostd"
    user = "MySql user"
  
  }
}		
  

resource "keboola_mysql_writer_table" "DemoMySqlWritertable" {
   writer_id = "${keboola_mysql_writer.MySqlWriterTest.id}"

       table{
      db_name = "Jontest_table2"
      table_id = "out.c-jontransformation.cars_population"
      export = true
      incremental = false
  // primary_key = ["tname"]
  // where_column= "total_cars"
  // changed_since = "-15 minutes"
  where_values = [" "]
        column{
          name = "name"
          db_name = "tname"
          type = "nvarchar"
          nullable = false
          default = "djon"
          size = "255"
        }

    }
         
}

*/
/*
resource "keboola_aws_s3bucket_writer" "S3Test" {
name = "S3 Bucket test"
description = "Description test"

s3_wr_parameters {

  	bucket="keboolabucket"
  	accesskeyId= "AKIAIUYKLY3S4A5ZDWIA"
		secretaccesskey = "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"

						
}
 
  
}


resource "keboola_aws_s3Bucket_table" "S3Testtable" {
 writer_id = "${keboola_aws_s3bucket_writer.S3Test.id}"
 table_id = "out.c-jontransformation.cars_population"
  name = "Jon Test"
  s3_row_parameters{
  //  prefix = ""

  }
}

*/

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
 //   load_type = "1"
    enabled = false
    sshPort = "12"
    sshHost = "T2est host For AWS Redshift"
    user = "T2est user AWSREDSHIFT SSH"
  }
}

resource "keboola_aws_redshift_writer_table" "DemoAWSRedshiftWritertable" {
   writer_id = "${keboola_awsredshift_writer.DemoAWSRedshiftWriter.id}"
   auto_run = true
    table{
      db_name = "Jontest_table2"
      table_id = "out.c-jontransformation.cars_population"
      export = true
      incremental = false
  // primary_key = ["tname"]
  // where_column= "total_cars"
  // changed_since = "-15 minutes"
 // where_values = [" "]
        column{
          name = "name"
          db_name = "tname"
          type = "nvarchar"
          nullable = false
          default = "djon"
          size = "255"
        }

    }
    
    
 
  
}

/*

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
    sshHost = "Test host For Sql Server"
    user = "Test user Sql Server ssh"
 
  }
}

resource "keboola_sqlserver_writer_tables" "SqlServerWriterTable4" {
   writer_id = "${keboola_sqlserver_writer.SqlServerWriter5.id}"
    table{
      db_name = "Jontest_table2"
      table_id = "out.c-jontransformation.cars_population"
      export = true
      incremental = false
  // primary_key = ["tname"]
 //  where_column= "total_cars"
 //  changed_since = "-15 minutes"
 // where_values = [" "]
        column{
          name = "population"
          db_name = "population"
          type = "nvarchar"
          nullable = true
          default = "jon"
          size = "25"
        }

    }
     table{
      db_name = "Jontest_table3"
      table_id = "in.c-keboola-ex-db-snowflake-500829435.cars"
      export = true
      incremental = false
  // primary_key = ["country"]
 //  where_column= "total_cars"
 //  changed_since = "-15 minutes"
//  where_values = [" "]
        column{
          name = "country"
          db_name = "country2"
          type = "nvarchar"
          nullable = false
          default = "jon"
          size = "255"
        }

    }
 
  
}
*/
