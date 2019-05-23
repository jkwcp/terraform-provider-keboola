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
    api_key = "6255-159236-hYtTi6f1j2mXeWKRlcz66ff8SB7QepUfUtXooV05"
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
        table_id=52172
        name="cars"
        incremental=false
        table_name="cars"
        schema="HELP_TUTORIAL"
  }
  table{
        table_id=52182
        name="countries"
        incremental=false
        table_name="countries"
        schema="HELP_TUTORIAL"
  }
}


resource "keboola_sqlserver_writer" "Demo_SqlServerWriter" {
  name  = "Demo SqlServerWriter"
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



resource "keboola_sqlserver_writer_tables" "Demo_SqlServerWriter_Table" {
   writer_id = "${keboola_sqlserver_writer.Demo_SqlServerWriter.id}"
    table{
      db_name = "Demo"
      table_id = "out.c-demo.cars_population"
      export = true
      incremental = false

        column{
          name = "name"
          db_name = "Country"
          type = "nvarchar"
          nullable = true
          default = "jon"
          size = "25"
        }
        column{
          name = "population"
          db_name = "population"
          type = "nvarchar"
          nullable = true
          default = "jon"
          size = "25"
        }
    column{
          name = "cars_per_capita"
          db_name = "cars_per_capita"
          type = "nvarchar"
          nullable = true
          default = "jon"
          size = "25"
        }
    }
 
}




resource "keboola_mysql_writer" "Demo_MySqlWriter" {
name = "Demo MySql Terraform"
description = "Description test"

mysql_wr_parameters{
   hostname = "jontestmysql.mysql.database.azure.com"
    port = "3306"
    username = "jon@jontestmysql"
    hashed_password = "#2702norland"
    database = "writer_sample"
    enabled = false
    sshPort = "11"
    sshHost = "MySql Host"
    user = "MySql user"
  
  }
}		


resource "keboola_mysql_writer_table" "Demo_MySqlWriter_Table" {
   writer_id = "${keboola_mysql_writer.Demo_MySqlWriter.id}"

       table{
      db_name = "Jontest_table2"
      table_id = "out.c-jontransformation.cars_population"
      export = true
      incremental = false

        column{
          name = "name"
          db_name = "tname"
          type = "nvarchar"
          nullable = false
          default = "test"
          size = "255"
        }

    }
         
}


resource "keboola_aws_s3bucket_writer" "Demo_S3Writer" {
name = " S3 Bucket Terraform Demo"
description = "Description test"

s3_wr_parameters {

  	bucket="keboolabucket"
  	accesskeyId= "AKIAJQTODJLLJADEIEIA"
		secretaccesskey = "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"

						
}
 
  
}


resource "keboola_aws_s3Bucket_table" "Demo_S3_Writer_table" {
 writer_id = "${keboola_aws_s3bucket_writer.Demo_S3Writer.id}"
 table_id = "out.c-jontransformation.cars_population"
  name = "Jon Test"
  s3_row_parameters{
  prefix = ""

  }
}



resource "keboola_awsredshift_writer" "Demo_AWSRedshift" {
  name  = "Redshift Terraform Demo"
  description = "This is an example of aws Redshift"
  redshift_wr_parameters{
    hostname = "joncluster.cs96vivrwyuv.us-west-2.redshift.amazonaws.com"
    port = "5439"
    username = "jon"
    hashed_password = "#2702Norland"
    database = "jondatabase"
    schema  = "public"

    enabled = false
    sshPort = "12"
    sshHost = "T2est host For AWS Redshift"
    user = "T2est user AWSREDSHIFT SSH"
  }
}

resource "keboola_aws_redshift_writer_table" "Demo_AWSRedshift_Table" {
   writer_id = "${keboola_awsredshift_writer.Demo_AWSRedshift.id}"
   auto_run = true
    table{
      db_name = "Jontest_table2"
      table_id = "out.c-jontransformation.cars_population"
      export = true
      incremental = false
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


