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
func resourceKeboolaMySqlWriterTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaMySQLWriterTablesCreate,
		Read:   resourceKeboolaMySQLTablesRead,
		Update: resourceKeboolaMySQLWriterTablesUpdate,
		Delete: resourceKeboolaMySQLWriterTablesDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"auto_run": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
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
						"changed_since": {
							Type:     schema.TypeString,
							Optional: true,
							Default:  "",
						},
						"where_column": {
							Type:     schema.TypeString,
							Optional: true,
							Default:  "",
						},
						"where_operator": {
							Type:     schema.TypeString,
							Optional: true,
							Default:  "eq",
						},
						"where_values": {
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
// Its suppose to create the table for the the My Sql componeent
//When does it get called:
// It gets called when the the resourceKeboolaMySQLWriterTables calls it
//Completed:
// No
func resourceKeboolaMySQLWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating My Sql Writer Tables in Keboola")

	client := meta.(*KBCClient)

	writerID := d.Get("writer_id").(string)
	tables := d.Get("table").(*schema.Set).List()

	mappedTables := make([]MySqlWriterTable, 0, len(tables))
	storageTables := make([]MySqlWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})
		mappedTable := MySqlWriterTable{
			DatabaseName: config["db_name"].(string),
			Export:       config["export"].(bool),
			TableID:      config["table_id"].(string),
			Incremental:  config["incremental"].(bool),
			//	LoadType:     config["load_type"].(string),
		}

		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		storageTable := MySqlWriterStorageTable{
			Source:      mappedTable.TableID,
			Destination: fmt.Sprintf("%s.csv", mappedTable.TableID),
		}
		if val, ok := config["changed_since"]; ok {
			storageTable.ChangedSince = val.(string)
		}
		if val, ok := config["where_column"]; ok {
			storageTable.WhereColumn = val.(string)
		}
		if val, ok := config["where_operator"]; ok {
			storageTable.WhereOperator = val.(string)
		}

		if q := config["where_values"]; q != nil {
			storageTable.WhereValues = AsStringArray(q.([]interface{}))
		}
		itemConfigs := config["column"].([]interface{})
		mappedItems := make([]MySqlWriterTableItem, 0, len(itemConfigs))
		columnsNames := make([]string, 0, len(itemConfigs))

		for _, item := range itemConfigs {
			itemConfig := item.(map[string]interface{})

			mappedItem := MySqlWriterTableItem{
				Name:         itemConfig["name"].(string),
				DatabaseName: itemConfig["db_name"].(string),
				Type:         itemConfig["type"].(string),
				Size:         itemConfig["size"].(string),
				IsNullable:   itemConfig["nullable"].(bool),
				DefaultValue: itemConfig["default"].(string),
			}
			mappedItems = append(mappedItems, mappedItem)
			columnsNames = append(columnsNames, mappedItem.Name)
		}
		mappedTable.Items = mappedItems
		storageTable.Columns = columnsNames

		mappedTables = append(mappedTables, mappedTable)
		storageTables = append(storageTables, storageTable)
	}

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", writerID))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var sqlserverwriter MySqlWriter

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
	updateMySQLForm := url.Values{}
	updateMySQLForm.Add("configuration", string(sqlserverConfigJSON))
	updateMySQLForm.Add("changeDescription", "Update My Sql tables")

	updateMySQLBuffer := buffer.FromForm(updateMySQLForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", writerID), updateMySQLBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	d.SetId(writerID)

	return resourceKeboolaMySQLTablesRead(d, meta)
}

//What does it do:
// Its suppose to Read and compare what the terraform script has and what the keboola provider has.
//When does it get called:
// it gets called with update and read
//Completed:
// No
func resourceKeboolaMySQLTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading My Sqlr Writer Tables from Keboola.")
	if d.Id() == "" {
		return nil
	}
	client := meta.(*KBCClient)

	getMySQLWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", d.Id()))

	if hasErrors(err, getMySQLWriterResponse) {
		if getMySQLWriterResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getMySQLWriterResponse)
	}

	var mysqlWriter MySqlWriter

	decoder := json.NewDecoder(getMySQLWriterResponse.Body)
	err = decoder.Decode(&mysqlWriter)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}

	storageInputTableMap := make(map[string]MySqlWriterStorageTable)

	for _, storageInputTable := range mysqlWriter.Configuration.Storage.Input.Tables {
		storageInputTableMap[storageInputTable.Source] = storageInputTable
	}

	for _, tableConfig := range mysqlWriter.Configuration.Parameters.Tables {
		storageInputTable := storageInputTableMap[tableConfig.TableID]
		tableDetails := map[string]interface{}{
			"db_Name":     tableConfig.DatabaseName,
			"export":      tableConfig.Export,
			"table_Id":    tableConfig.TableID,
			"incremental": tableConfig.Incremental,
			"primary_key": tableConfig.PrimaryKey,

			"changed_since":  storageInputTable.ChangedSince,
			"where_column":   storageInputTable.WhereColumn,
			"where_operator": storageInputTable.WhereOperator,
			"where_values":   storageInputTable.WhereValues,
		}

		var items []map[string]interface{}
		for _, item := range tableConfig.Items {
			itemDetails := map[string]interface{}{
				"name":     item.Name,
				"dbName":   item.DatabaseName,
				"type":     item.Type,
				"size":     item.Size,
				"nullable": item.IsNullable,
				"default":  item.DefaultValue,
			}

			items = append(items, itemDetails)
		}
		tableDetails["column"] = items

		tables = append(tables, tableDetails)
	}

	d.Set("table", tables)
	if d.Get("auto_run") == true {
		mysqlConfigJSON, err := json.Marshal(mysqlWriter)

		MySqlWriterRunResponse, err := client.PostToDockerRun("keboola.wr-db-mysql", d.Id(), mysqlConfigJSON)
		if hasErrors(err, MySqlWriterRunResponse) {
			return extractError(err, MySqlWriterRunResponse)
		}
	}
	return nil
}

//What does it do:
// Its suppose to update the table
//When does it get called:
// when the resourceKeboolaMySQLWriterTables gets called
//Completed:
// Yes
func resourceKeboolaMySQLWriterTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating My Sql Writer table in Keboola.")

	tables := d.Get("table").(*schema.Set).List()

	mappedTables := make([]MySqlWriterTable, 0, len(tables))
	storageTables := make([]MySqlWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := MySqlWriterTable{
			DatabaseName: config["db_name"].(string),
			Export:       config["export"].(bool),
			TableID:      config["table_id"].(string),
			Incremental:  config["incremental"].(bool),
		}
		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		storageTable := MySqlWriterStorageTable{
			Source:      mappedTable.TableID,
			Destination: fmt.Sprintf("%s.csv", mappedTable.TableID),
		}
		if val, ok := config["changed_since"]; ok {
			storageTable.ChangedSince = val.(string)
		}
		if val, ok := config["where_column"]; ok {
			storageTable.WhereColumn = val.(string)
		}
		if val, ok := config["where_operator"]; ok {
			storageTable.WhereOperator = val.(string)
		}

		if q := config["where_values"]; q != nil {
			storageTable.WhereValues = AsStringArray(q.([]interface{}))
		}

		itemConfigs := config["column"].([]interface{})
		mappedColumns := make([]MySqlWriterTableItem, 0, len(itemConfigs))
		columnNames := make([]string, 0, len(itemConfigs))
		for _, item := range itemConfigs {
			columnConfig := item.(map[string]interface{})

			mappedColumn := MySqlWriterTableItem{
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

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var sqlserverWriter MySqlWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&sqlserverWriter)

	if err != nil {
		return err
	}

	sqlserverWriter.Configuration.Parameters.Tables = mappedTables
	sqlserverWriter.Configuration.Storage.Input.Tables = storageTables

	sqlserverConfigJSON, err := json.Marshal(sqlserverWriter.Configuration)

	if err != nil {
		return err
	}

	updateMySQLForm := url.Values{}
	updateMySQLForm.Add("configuration", string(sqlserverConfigJSON))
	updateMySQLForm.Add("changeDescription", "Update My Sql tables")

	updateMySQLBuffer := buffer.FromForm(updateMySQLForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", d.Id()), updateMySQLBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaMySQLTablesRead(d, meta)
}

//What does it do:
// it destory the terraform connection when the code block is mvoed from terraform
//When does it get called:
// From the resourceKeboolaMySQLWriterTables
//Completed:
// Yes

func resourceKeboolaMySQLWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing My Sql Writer Tables in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {

		return extractError(err, getWriterResponse)
	}

	var sqlserverWriter MySqlWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&sqlserverWriter)

	if err != nil {
		return err

	}

	var emptyTables []MySqlWriterTable
	sqlserverWriter.Configuration.Parameters.Tables = emptyTables

	var emptyStorageTable []MySqlWriterStorageTable
	sqlserverWriter.Configuration.Storage.Input.Tables = emptyStorageTable

	sqlserverConfigJSON, err := json.Marshal(sqlserverWriter.Configuration)

	if err != nil {
		return err
	}

	clearMySQLTableForm := url.Values{}
	clearMySQLTableForm.Add("configuration", string(sqlserverConfigJSON))
	clearMySQLTableForm.Add("changeDescription", "Update MySQL tables")

	clearMySQLTablesBuffer := buffer.FromForm(clearMySQLTableForm)

	clearResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-mysql/configs/%s", d.Id()), clearMySQLTablesBuffer)
	if hasErrors(err, clearResponse) {
		return extractError(err, clearResponse)
	}
	if hasErrors(err, clearResponse) {
		return extractError(err, clearResponse)
	}
	return nil
}
