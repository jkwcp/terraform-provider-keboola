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
  api_key = "6017-153345-QFi6wl2m0Saf8Tm9kG1lET09oQjeRNnDqP9icOv4"
}

resource "keboola_dropbox_writer" "AndyDropBoxTest2" {
   name  = "AndyDropBoxTest2"
     description = "AndyTest_Dropbox"
}
resource "keboola_tableau_writer" "AndyTableauTest2" {
   name  = "AndyTableauTest2"
     description = "AndyTest_Tableau"
}
