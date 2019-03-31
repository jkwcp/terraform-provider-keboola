provider "keboola" {
  api_key = "5959-152644-fWyZASDSe52fQmq5mZPD4tmrfmnDSxQMXKD1Llj5"
}


resource "keboola_googledrive_extractor" "test1" {
  name  = "Example google"
}

resource "keboola_googledrive_extractor" "test2" {
  name  = "g2"
}


resource "keboola_snowflake_extractor" "test3" {
  name = "sss1"
}
