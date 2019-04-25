package keboola

//this isn't complete
import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
)

//Specifies the Create, Read, Update, and Delete functions for the Tableau Writer table
//Called from main.tf
//Currently incomplete, attempting to reconfigure based on the aws_redshift_writer_table.go file
func resourceKeboolaTableauWriterTables() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaTableauWriterTablesCreate,
		Read:   resourceKeboolaTableauWriterTablesRead,
		Update: resourceKeboolaTableauWriterTablesUpdate,
		Delete: resourceKeboolaTableauWriterTablesDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"table": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"db_name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"export": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"table_id": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"incremental": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"primary_key": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"column": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"db_name": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"type": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"size": {
										Type:     schema.TypeString,
										Required: true,
									},
									"nullable": {
										Type:     schema.TypeBool,
										Optional: true,
										Default:  false,
									},
									"default": {
										Type:     schema.TypeString,
										Optional: true,
										Default:  "",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

//Creates a Tableau Writer table in Keboola Connection platform in the project
//Called from main.tf after resource_keboola_tableau_writer.go is called
//Currently incomplete, attempting to reconfigure based on the aws_redshift_writer_table.go file
func resourceKeboolaTableauWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Tableau Writer Tables in Keboola.")

	writerID := d.Get("writer_id").(string)
	// tables := d.Get("table").(*schema.Set).List()

	// mappedTables := make([]TableauWriterTable, 0, len(tables))
	// storageTables := make([]TableauWriterStorageTable, 0, len(tables))

	// for _, table := range tables {
	// 	config := table.(map[string]interface{})

	// 	mappedTable := TableauWriterTable{
	// 		DatabaseName: config["db_name"].(string),
	// 		Export:       config["export"].(bool),
	// 		TableID:      config["table_id"].(string),
	// 		Incremental:  config["incremental"].(bool),
	// 	}

	// 	if q := config["primary_key"]; q != nil {
	// 		mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
	// 	}

	// 	storageTable := TableauWriterStorageTable{
	// 		Source:      mappedTable.TableID,
	// 		Destination: fmt.Sprintf("%s.csv", mappedTable.TableID),
	// 	}

	// 	columnConfigs := config["column"].([]interface{})
	// 	mappedColumns := make([]TableauWriterTableItem, 0, len(columnConfigs))
	// 	columnNames := make([]string, 0, len(columnConfigs))
	// 	for _, column := range columnConfigs {
	// 		columnConfig := column.(map[string]interface{})

	// 		mappedColumn := TableauWriterTableItem{
	// 			Name:         columnConfig["name"].(string),
	// 			DatabaseName: columnConfig["db_name"].(string),
	// 			Type:         columnConfig["type"].(string),
	// 			Size:         columnConfig["size"].(string),
	// 			IsNullable:   columnConfig["nullable"].(bool),
	// 			DefaultValue: columnConfig["default"].(string),
	// 		}

	// 		mappedColumns = append(mappedColumns, mappedColumn)
	// 		columnNames = append(columnNames, mappedColumn.Name)
	// 	}

	// 	mappedTable.Items = mappedColumns
	// 	storageTable.Columns = columnNames

	// 	mappedTables = append(mappedTables, mappedTable)
	// 	storageTables = append(storageTables, storageTable)
	// }

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", writerID))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var TableauWriter TableauWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&TableauWriter)

	if err != nil {
		return err
	}

	// TableauWriter.Configuration.Parameters.Tables = mappedTables
	// TableauWriter.Configuration.Storage.Input.Tables = storageTables

	// TableauConfigJSON, err := json.Marshal(TableauWriter.Configuration)

	// 	if err != nil {
	// 		return err
	// 	}

	// 	updateTableauForm := url.Values{}
	// 	updateTableauForm.Add("configuration", string(TableauConfigJSON))
	// 	updateTableauForm.Add("changeDescription", "Update Redshift tables")

	// 	updateTableauBuffer := buffer.FromForm(updateTableauForm)

	// 	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", writerID), updateTableauBuffer)

	// 	if hasErrors(err, updateResponse) {
	// 		return extractError(err, updateResponse)
	// 	}

	// 	d.SetId(writerID)

	return resourceKeboolaTableauWriterTablesRead(d, meta)
}

//Reads from a Tableau Writer table in Keboola Connection platform in the project and updates the table if the table configurations are different
//Called from main.tf after resource_keboola_tableau_writer.go is called
//Currently incomplete, attempting to reconfigure based on the aws_redshift_writer_table.go file
func resourceKeboolaTableauWriterTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Tableau Writer Tables from Keboola.")

	// if d.Id() == "" {
	// 	return nil
	// }

	// client := meta.(*KBCClient)

	// 	getTableauWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.keboola.wr-redshift-v2/configs/%s", d.Id()))

	// 	if hasErrors(err, getTableauWriterResponse) {
	// 		if getTableauWriterResponse.StatusCode == 404 {
	// 			d.SetId("")
	// 			return nil
	// 		}

	// 		return extractError(err, getTableauWriterResponse)
	// 	}

	// 	var TableauWriter TableauWriter

	// 	decoder := json.NewDecoder(getTableauWriterResponse.Body)
	// 	err = decoder.Decode(&TableauWriter)

	// 	if err != nil {
	// 		return err
	// 	}

	// 	var tables []map[string]interface{}

	// 	for _, tableConfig := range TableauWriter.Configuration.Parameters.Tables {
	// 		tableDetails := map[string]interface{}{
	// 			"db_name":     tableConfig.DatabaseName,
	// 			"export":      tableConfig.Export,
	// 			"table_id":    tableConfig.TableID,
	// 			"incremental": tableConfig.Incremental,
	// 			"primary_key": tableConfig.PrimaryKey,
	// 		}

	// 		var columns []map[string]interface{}

	// 		for _, item := range tableConfig.Items {
	// 			columnDetails := map[string]interface{}{
	// 				"name":     item.Name,
	// 				"db_name":  item.DatabaseName,
	// 				"type":     item.Type,
	// 				"size":     item.Size,
	// 				"nullable": item.IsNullable,
	// 				"default":  item.DefaultValue,
	// 			}

	// 			columns = append(columns, columnDetails)
	// 		}

	// 		tableDetails["column"] = columns

	// 		tables = append(tables, tableDetails)
	// 	}

	// 	d.Set("table", tables)

	return nil
}

//Updates a Tableau Writer table in Keboola Connection platform in the project
//Called from main.tf after resource_keboola_tableau_writer.go is called
//Currently incomplete, attempting to reconfigure based on the aws_redshift_writer_table.go file
func resourceKeboolaTableauWriterTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Tableau Writer Tables in Keboola.")

	// tables := d.Get("table").([]interface{})

	// mappedTables := make([]TableauWriterTable, 0, len(tables))
	// storageTables := make([]TableauWriterStorageTable, 0, len(tables))

	// for _, table := range tables {
	// config := table.(map[string]interface{})

	// mappedTable := TableauWriterTable{
	// 	DatabaseName: config["db_name"].(string),
	// 	Export:       config["export"].(bool),
	// 	TableID:      config["table_id"].(string),
	// 	Incremental:  config["incremental"].(bool),
	// }

	// if q := config["primary_key"]; q != nil {
	// 	mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
	// }

	// storageTable := TableauWriterStorageTable{
	// 	Source:      mappedTable.TableID,
	// 	Destination: fmt.Sprintf("%s.csv", mappedTable.TableID),
	// }

	// 	columnConfigs := config["column"].([]interface{})
	// 	mappedColumns := make([]TableauWriterTableItem, 0, len(columnConfigs))
	// 	columnNames := make([]string, 0, len(columnConfigs))
	// 	for _, column := range columnConfigs {
	// 		columnConfig := column.(map[string]interface{})

	// 		mappedColumn := TableauWriterTableItem{
	// 			Name:         columnConfig["name"].(string),
	// 			DatabaseName: columnConfig["db_name"].(string),
	// 			Type:         columnConfig["type"].(string),
	// 			Size:         columnConfig["size"].(string),
	// 			IsNullable:   columnConfig["nullable"].(bool),
	// 			DefaultValue: columnConfig["default"].(string),
	// 		}

	// 		mappedColumns = append(mappedColumns, mappedColumn)
	// 		columnNames = append(columnNames, mappedColumn.Name)
	// 	}

	// 	mappedTable.Items = mappedColumns
	// 	storageTable.Columns = columnNames

	// 	mappedTables = append(mappedTables, mappedTable)
	// 	storageTables = append(storageTables, storageTable)
	// }

	// client := meta.(*KBCClient)

	// getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()))

	// if hasErrors(err, getWriterResponse) {
	// 	return extractError(err, getWriterResponse)
	// }

	// var TableauWriter TableauWriter

	// decoder := json.NewDecoder(getWriterResponse.Body)
	// err = decoder.Decode(&TableauWriter)

	// if err != nil {
	// 	return err
	// }

	// TableauWriter.Configuration.Parameters.Tables = mappedTables
	// TableauWriter.Configuration.Storage.Input.Tables = storageTables

	// TableauConfigJSON, err := json.Marshal(TableauWriter.Configuration)

	// if err != nil {
	// 	return err
	// }

	// updateCredentialsForm := url.Values{}
	// updateCredentialsForm.Add("name", d.Get("name").(string))
	// updateCredentialsForm.Add("description", d.Get("description").(string))
	// updateCredentialsForm.Add("configuration", string(TableauConfigJSON))
	// updateCredentialsForm.Add("changeDescription", "Updated Tableau Writer configuration via Terraform")

	// updateTableauBuffer := buffer.FromForm(updateCredentialsForm)

	// updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()), updateTableauBuffer)

	// if hasErrors(err, updateResponse) {
	// return extractError(err, updateResponse)
	return resourceKeboolaTableauWriterTablesRead(d, meta)
}

//Deletes a Tableau Writer table in Keboola Connection platform in the project
//Called from main.tf after resource_keboola_tableau_writer.go is called
//Currently incomplete, attempting to reconfigure based on the aws_redshift_writer_table.go file
func resourceKeboolaTableauWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing Tableau Writer Tables in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var TableauWriter TableauWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&TableauWriter)

	if err != nil {
		return err
	}

	// var emptyTables []TableauWriterTable
	// TableauWriter.Configuration.Parameters.Tables = emptyTables

	// var emptyStorageTables []TableauWriterStorageTable
	// TableauWriter.Configuration.Storage.Input.Tables = emptyStorageTables

	// TableauConfigJSON, err := json.Marshal(TableauWriter.Configuration)

	if err != nil {
		return err
	}

	// clearTableauTablesForm := url.Values{}
	// clearTableauTablesForm.Add("configuration", string(TableauConfigJSON))
	// clearTableauTablesForm.Add("changeDescription", "Update Tableau tables")

	// clearTableauTablesBuffer := buffer.FromForm(clearTableauTablesForm)

	// clearResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()), clearTableauTablesBuffer)

	// if hasErrors(err, clearResponse) {
	// 	return extractError(err, clearResponse)
	// }

	// d.SetId("")

	return nil
}
