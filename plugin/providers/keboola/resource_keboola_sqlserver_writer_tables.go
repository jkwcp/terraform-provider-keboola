package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//What does it do:
//It creates a resource for sqlwriter talbe
//When does it get called:
//it gets called from the propvider when the terraform script calls the provider
//Completed:
// No
func resourceKeboolaSQLServerWriterTables() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSQLServerWriterTablesCreate,
		Read:   resourceKeboolaSQLServerTablesRead,
		Update: resourceKeboolaSQLServerWriterTablesUpdate,
		Delete: resourceKeboolaSQLServerWriterTablesDelete,

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

//What does it do:
// Its suppose to create the table for the the sql server componeent
//When does it get called:
// It gets called when the the resourceKeboolaSQLServerWriterTables calls it
//Completed:
// No
func resourceKeboolaSQLServerWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating SQL Server Writer Tables in Keboola")

	client := meta.(*KBCClient)

	writerID := d.Get("writer_id").(string)
	tables := d.Get("table").(*schema.Set).List()

	mappedTables := make([]SQLServerWriterTable, 0, len(tables))
	storageTables := make([]SQLServerWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})
		mappedTable := SQLServerWriterTable{
			DatabaseName: config["db_name"].(string),
			Export:       config["export"].(bool),
			TableID:      config["table_id"].(string),
			Incremental:  config["incremental"].(bool),
		}

		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		storageTable := SQLServerWriterStorageTable{
			Source:      mappedTable.TableID,
			Destination: fmt.Sprintf("%s.csv", mappedTable.TableID),
		}
		columnConfigs := config["column"].([]interface{})
		mappedColumns := make([]SQLServerWriterTableItem, 0, len(columnConfigs))
		columnNames := make([]string, 0, len(columnConfigs))

		for _, column := range columnConfigs {
			columnConfig := column.(map[string]interface{})

			mappedColumn := SQLServerWriterTableItem{
				Name:         columnConfig["name"].(string),
				DatabaseName: columnConfig["db_name"].(string),
				Type:         columnConfig["type"].(string),
				Size:         columnConfig["size"].(string),
				IsNullable:   columnConfig["nullable"].(bool),
				DefaultValue: columnConfig["default"].(string),
			}
			mappedColumns = append(mappedColumns, mappedColumn)
			columnNames = append(columnNames, mappedColumn.Name)
		}
		mappedTable.Items = mappedColumns
		storageTable.Columns = columnNames

		mappedTables = append(mappedTables, mappedTable)
		storageTables = append(storageTables, storageTable)
	}

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mssql-v2/configs/%s", writerID))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var sqlserverwriter SQLServerWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&sqlserverwriter)

	if err != nil {
		return err
	}
	sqlserverwriter.Configuration.Parameters.Tables = mappedTables
	sqlserverwriter.Configuration.Storage.Input.Tables = storageTables

	sqlserverConfigJSON, err := json.Marshal(sqlserverwriter.Configuration)

	if err != nil {
		return err
	}
	updateSQLServerForm := url.Values{}
	updateSQLServerForm.Add("configuration", string(sqlserverConfigJSON))
	updateSQLServerForm.Add("changeDescription", "Update SQL Server tables")

	updateSQLServerBuffer := buffer.FromForm(updateSQLServerForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-mssql-v2/configs/%s", writerID), updateSQLServerBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	d.SetId(writerID)

	return resourceKeboolaSQLServerTablesRead(d, meta)
}

//What does it do:
// Its suppose to Read and compare what the terraform script has and what the keboola provider has.
//When does it get called:
// it gets called with update and read
//Completed:
// No
func resourceKeboolaSQLServerTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading SQL Serverr Writer Tables from Keboola.")
	if d.Id() == "" {
		return nil
	}
	client := meta.(*KBCClient)
	getSQLServerWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mssql-v2/configs/%s", d.Id()))

	if hasErrors(err, getSQLServerWriterResponse) {
		if getSQLServerWriterResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getSQLServerWriterResponse)
	}
	var sqlserverWriter SQLServerWriter

	decoder := json.NewDecoder(getSQLServerWriterResponse.Body)
	err = decoder.Decode(&sqlserverWriter)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}

	for _, tableConfig := range sqlserverWriter.Configuration.Parameters.Tables {
		tableDetails := map[string]interface{}{
			"db_name":     tableConfig.DatabaseName,
			"export":      tableConfig.Export,
			"table_id":    tableConfig.TableID,
			"incremental": tableConfig.Incremental,
			"primary_key": tableConfig.PrimaryKey,
		}
		var columns []map[string]interface{}
		for _, item := range tableConfig.Items {
			columnDetails := map[string]interface{}{
				"name":     item.Name,
				"db_name":  item.DatabaseName,
				"type":     item.Type,
				"size":     item.Size,
				"nullable": item.IsNullable,
				"default":  item.DefaultValue,
			}

			columns = append(columns, columnDetails)
		}
		tableDetails["column"] = columns

		tables = append(tables, tableDetails)
	}

	d.Set("table", tables)

	return nil
}

//What does it do:
// Its suppose to update the table
//When does it get called:
// when the resourceKeboolaSQLServerWriterTables gets called
//Completed:
// No
func resourceKeboolaSQLServerWriterTablesUpdate(d *schema.ResourceData, meta interface{}) error {

	return nil
	//return resourceKeboolaSnowflakeWriterTablesRead(d, meta)
}

//What does it do:
// it destory the terraform connection when the code block is mvoed from terraform
//When does it get called:
// From the resourceKeboolaSQLServerWriterTables
//Completed:
// No

func resourceKeboolaSQLServerWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {

	return nil
}
