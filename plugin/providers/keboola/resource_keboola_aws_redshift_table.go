package keboola

//this isn't complete
import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaAWSRedShiftWriterTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAWSRedShiftWriterTablesCreate,
		Read:   resourceKeboolaAWSRedShiftWriterTablesRead,
		Update: resourceKeboolaAWSRedShiftWriterTablesUpdate,
		Delete: resourceKeboolaAWSRedShiftWriterTablesDelete,

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
func resourceKeboolaAWSRedShiftWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating AWS RedShift Writer Tables in Keboola.")

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
		columnConfigs := config["column"].([]interface{})
		mappedColumns := make([]AWSRedShiftWriterTableItem, 0, len(columnConfigs))
		columnNames := make([]string, 0, len(columnConfigs))
		for _, column := range columnConfigs {
			columnConfig := column.(map[string]interface{})

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

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", writerID))

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

	updateAwsredshiftForm := url.Values{}
	updateAwsredshiftForm.Add("configuration", string(awsredshiftConfigJSON))
	updateAwsredshiftForm.Add("changeDescription", "Update Redshift tables")

	updateAwsredshiftBuffer := buffer.FromForm(updateAwsredshiftForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", writerID), updateAwsredshiftBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	d.SetId(writerID)

	return resourceKeboolaAWSRedShiftWriterTablesRead(d, meta)
}

func resourceKeboolaAWSRedShiftWriterTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading AWS RedShift Writer Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)

	getAWSRedShiftWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.keboola.wr-redshift-v2/configs/%s", d.Id()))

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
			"db_name":        tableConfig.DatabaseName,
			"export":         tableConfig.Export,
			"table_id":       tableConfig.TableID,
			"incremental":    tableConfig.Incremental,
			"primary_key":    tableConfig.PrimaryKey,
			"changed_since":  storageInputTable.ChangedSince,
			"where_column":   storageInputTable.WhereColumn,
			"where_operator": storageInputTable.WhereOperator,
			"where_values":   storageInputTable.WhereValues,
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

func resourceKeboolaAWSRedShiftWriterTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating AWS RedShift Writer Tables in Keboola.")

	tables := d.Get("table").([]interface{})

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
		columnConfigs := config["column"].([]interface{})
		mappedColumns := make([]AWSRedShiftWriterTableItem, 0, len(columnConfigs))
		columnNames := make([]string, 0, len(columnConfigs))
		for _, column := range columnConfigs {
			columnConfig := column.(map[string]interface{})

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

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("configuration", string(awsredshiftConfigJSON))
	updateCredentialsForm.Add("changeDescription", "Updated AWS RedShift Writer configuration via Terraform")

	updateAWSRedShiftBuffer := buffer.FromForm(updateCredentialsForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()), updateAWSRedShiftBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaAWSRedShiftWriterTablesRead(d, meta)
}

func resourceKeboolaAWSRedShiftWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing AWS RedShift Writer Tables in Keboola: %s", d.Id())

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

	var emptyStorageTables []AWSRedShiftWriterStorageTable
	awsredshiftWriter.Configuration.Storage.Input.Tables = emptyStorageTables

	awsredshiftConfigJSON, err := json.Marshal(awsredshiftWriter.Configuration)

	if err != nil {
		return err
	}

	clearAWSRedShiftTablesForm := url.Values{}
	clearAWSRedShiftTablesForm.Add("configuration", string(awsredshiftConfigJSON))
	clearAWSRedShiftTablesForm.Add("changeDescription", "Update AWS REDShift tables")

	clearAWSRedShiftTablesBuffer := buffer.FromForm(clearAWSRedShiftTablesForm)

	clearResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-redshift-v2/configs/%s", d.Id()), clearAWSRedShiftTablesBuffer)

	if hasErrors(err, clearResponse) {
		return extractError(err, clearResponse)
	}

	d.SetId("")

	return nil
}
