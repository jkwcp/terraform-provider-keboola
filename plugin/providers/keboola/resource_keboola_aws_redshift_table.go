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
//It creates a resource for AWS Redshift talbe
//When does it get called:
//it gets called from the provider when the terraform script is executed and it calls the provider
//Completed:
// Yes
func resourceKeboolaAWSRedShiftWriterTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAWSRedShiftWriterTablesCreate,
		Read:   resourceKeboolaAWSRedShiftTablesRead,
		Update: resourceKeboolaAWSRedShiftWriterTablesUpdate,
		Delete: resourceKeboolaAWSRedShiftWriterTablesDelete,

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
// Its creates the table for the the RedShfit componeent
//When does it get called:
// It gets called when the the resourceKeboolaAWSRedShiftWriterTables calls it
//Completed:
// YES
func resourceKeboolaAWSRedShiftWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating AWS RedShift Tables in Keboola")

	client := meta.(*KBCClient)

	writerID := d.Get("writer_id").(string)
	tables := d.Get("table").(*schema.Set).List()

	mappedTables := make([]AWSRedShiftWriterTable, 0, len(tables))
	storageTables := make([]AWSRedShiftWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})
		mappedTable := AWSRedShiftWriterTable{
			DatabaseName: config["db_name"].(string),
			Export:       config["export"].(bool),
			TableID:      config["table_id"].(string),
			Incremental:  config["incremental"].(bool),
		}

		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		storageTable := AWSRedShiftWriterStorageTable{
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
		mappedItems := make([]AWSRedShiftWriterTableItem, 0, len(itemConfigs))
		columnsNames := make([]string, 0, len(itemConfigs))

		for _, item := range itemConfigs {
			itemConfig := item.(map[string]interface{})

			mappedItem := AWSRedShiftWriterTableItem{
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

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", writerID))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var awsredshiftwriter AWSRedShiftWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&awsredshiftwriter)

	if err != nil {
		return err
	}
	awsredshiftwriter.Configuration.Parameters.Tables = mappedTables
	awsredshiftwriter.Configuration.Storage.Input.Tables = storageTables

	awsredshiftConfigJSON, err := json.Marshal(awsredshiftwriter.Configuration)

	if err != nil {
		return err
	}
	updateAWSRedShiftForm := url.Values{}
	updateAWSRedShiftForm.Add("configuration", string(awsredshiftConfigJSON))
	updateAWSRedShiftForm.Add("changeDescription", "Update AWSRedshift tables")

	updateAWSRedShiftBuffer := buffer.FromForm(updateAWSRedShiftForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", writerID), updateAWSRedShiftBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	d.SetId(writerID)

	return resourceKeboolaAWSRedShiftTablesRead(d, meta)
}

//What does it do:
// Its suppose to Read and compare what is on the platform and what the terraform script has.  Also it has an auto run option which allows you to run the process automatically
//When does it get called:
// it gets called with resourceKeboolaAWSRedShiftWriterTablesUpdate and resourceKeboolaAWSRedShiftWriterTablesCreate
//Completed:
// No
func resourceKeboolaAWSRedShiftTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading  AWS RedShift Tables from Keboola.")
	if d.Id() == "" {
		return nil
	}
	client := meta.(*KBCClient)

	getAWSRedShiftWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()))

	if hasErrors(err, getAWSRedShiftWriterResponse) {
		if getAWSRedShiftWriterResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getAWSRedShiftWriterResponse)
	}

	var awsredshiftWriter AWSRedShiftWriter

	decoder := json.NewDecoder(getAWSRedShiftWriterResponse.Body)
	err = decoder.Decode(&awsredshiftWriter)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}

	storageInputTableMap := make(map[string]AWSRedShiftWriterStorageTable)

	for _, storageInputTable := range awsredshiftWriter.Configuration.Storage.Input.Tables {
		storageInputTableMap[storageInputTable.Source] = storageInputTable
	}

	for _, tableConfig := range awsredshiftWriter.Configuration.Parameters.Tables {
		storageInputTable := storageInputTableMap[tableConfig.TableID]
		tableDetails := map[string]interface{}{
			"db_Name":        tableConfig.DatabaseName,
			"export":         tableConfig.Export,
			"table_Id":       tableConfig.TableID,
			"incremental":    tableConfig.Incremental,
			"primary_key":    tableConfig.PrimaryKey,
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
		MySqlWriterRunResponse, err := client.PostToDockerRun("keboola.wr-redshift-v2", d.Id())
		if hasErrors(err, MySqlWriterRunResponse) {
			return extractError(err, MySqlWriterRunResponse)
		}
	}
	return nil
}

//What does it do:
// Its suppose to update the table if any changes where made on the platform and on the terraform script
//When does it get called:
// when the resourceKeboolaAWSRedShiftWriterTables gets called
//Completed:
// Yes
func resourceKeboolaAWSRedShiftWriterTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating  AWS RedShift table in Keboola.")

	tables := d.Get("table").(*schema.Set).List()

	mappedTables := make([]AWSRedShiftWriterTable, 0, len(tables))
	storageTables := make([]AWSRedShiftWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := AWSRedShiftWriterTable{
			DatabaseName: config["db_name"].(string),
			Export:       config["export"].(bool),
			TableID:      config["table_id"].(string),
			Incremental:  config["incremental"].(bool),
		}
		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		storageTable := AWSRedShiftWriterStorageTable{
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
		mappedColumns := make([]AWSRedShiftWriterTableItem, 0, len(itemConfigs))
		columnNames := make([]string, 0, len(itemConfigs))
		for _, item := range itemConfigs {
			columnConfig := item.(map[string]interface{})

			mappedColumn := AWSRedShiftWriterTableItem{
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

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var awsredshiftWriter AWSRedShiftWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&awsredshiftWriter)

	if err != nil {
		return err
	}

	awsredshiftWriter.Configuration.Parameters.Tables = mappedTables
	awsredshiftWriter.Configuration.Storage.Input.Tables = storageTables

	awsredshiftConfigJSON, err := json.Marshal(awsredshiftWriter.Configuration)

	if err != nil {
		return err
	}

	updateAWSRedShiftForm := url.Values{}
	updateAWSRedShiftForm.Add("configuration", string(awsredshiftConfigJSON))
	updateAWSRedShiftForm.Add("changeDescription", "Update RedShift tables")

	updateAWSRedShiftBuffer := buffer.FromForm(updateAWSRedShiftForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()), updateAWSRedShiftBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaAWSRedShiftTablesRead(d, meta)
}

//What does it do:
// it destory the terraform connection when the code block is moveded from terraform
//When does it get called:
// From the resourceKeboolaAWSRedShiftWriterTables
//Completed:
// Yes

func resourceKeboolaAWSRedShiftWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing AWS RedShift Tables in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {

		return extractError(err, getWriterResponse)
	}

	var awsredshiftWriter AWSRedShiftWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&awsredshiftWriter)

	if err != nil {
		return err

	}

	var emptyTables []AWSRedShiftWriterTable
	awsredshiftWriter.Configuration.Parameters.Tables = emptyTables

	var emptyStorageTable []AWSRedShiftWriterStorageTable
	awsredshiftWriter.Configuration.Storage.Input.Tables = emptyStorageTable

	awsredshiftConfigJSON, err := json.Marshal(awsredshiftWriter.Configuration)

	if err != nil {
		return err
	}

	clearAWSRedShiftTableForm := url.Values{}
	clearAWSRedShiftTableForm.Add("configuration", string(awsredshiftConfigJSON))
	clearAWSRedShiftTableForm.Add("changeDescription", "Update AWSRedShift tables")

	clearAWSRedShiftTablesBuffer := buffer.FromForm(clearAWSRedShiftTableForm)

	clearResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()), clearAWSRedShiftTablesBuffer)
	if hasErrors(err, clearResponse) {
		return extractError(err, clearResponse)
	}
	if hasErrors(err, clearResponse) {
		return extractError(err, clearResponse)
	}
	return nil
}
