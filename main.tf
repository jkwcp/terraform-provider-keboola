#add your own configurations

#must include api_key, look like this:

#provider "keboola" {
#  api_key = "6017-153348-I39OH0o6ZC6Gcg3hdPqIJbojCWRgb4XLgoXRS1mz"
#}

#Configurations will be like:

#resource "keboola_component_writer" "Component Name" {
#     Configurations....  
#}
provider "keboola" {
  api_key = "6017-153348-I39OH0o6ZC6Gcg3hdPqIJbojCWRgb4XLgoXRS1mz"
}
