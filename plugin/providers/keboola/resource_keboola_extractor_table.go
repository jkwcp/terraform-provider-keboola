package keboola

//This is complete
import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//region Keboola API Contracts

// Main function to the resource Snowflake Extractor Table.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaExtractorTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaExtractorTableCreate,
		Read:   resourceKeboolaExtractorTableRead,
		Update: resourceKeboolaExtractorTableUpdate,
		Delete: resourceKeboolaExtractorTableDelete,

		Schema: map[string]*schema.Schema{
			"extractor_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"table": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"table_id": {
							Type:     schema.TypeInt,
							Optional: true,
						},
						"incremental": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"enabled": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  true,
						},
						"output": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"primary_key": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"columns": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"schema": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"table_name": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
		},
	}
}

// Create function to the resource Snowflake Extractor Table.
// It gets called when the terraform applies a new Snowflake Extractor Table configuration.
// Completed
func resourceKeboolaExtractorTableCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Extractor tables in Keboola.")

	extractorID := d.Get("extractor_id").(string)
	tables := d.Get("table").(*schema.Set).List()

	mappedTables := make([]ExtractorTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		outputTable := "in.c-keboola-ex-db-snowflake-" + extractorID + "." + config["name"].(string)

		mappedTable := ExtractorTable{
			Name:        config["name"].(string),
			TableID:     config["table_id"].(int),
			Incremental: config["incremental"].(bool),
			OutputTable: outputTable,
			Enabled:     config["enabled"].(bool),
		}

		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		if q := config["columns"]; q != nil {
			mappedTable.Columns = AsStringArray(q.([]interface{}))
		}

		sTable := SnowflakeExtractorDatabaseTable{
			Schema:    config["schema"].(string),
			TableName: config["table_name"].(string),
		}

		mappedTable.Table = sTable

		mappedTables = append(mappedTables, mappedTable)
	}

	client := meta.(*KBCClient)
	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", extractorID))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var snowFlakeExtractor SnowFlakeExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&snowFlakeExtractor)

	if err != nil {
		return err
	}

	snowFlakeExtractor.Configuration.Parameters.Tables = mappedTables

	snowflakeConfigJSON, err := json.Marshal(snowFlakeExtractor.Configuration)

	if err != nil {
		return err
	}

	updateSnowflakeForm := url.Values{}
	updateSnowflakeForm.Add("configuration", string(snowflakeConfigJSON))
	updateSnowflakeForm.Add("changeDescription", "Update Snowflake tables")

	updateSnowflakeBuffer := buffer.FromForm(updateSnowflakeForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", extractorID), updateSnowflakeBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	d.SetId(extractorID)

	return resourceKeboolaExtractorTableRead(d, meta)
}

// Read function to the resource Snowflake Extractor Table.
// It gets called whenever there is an existing Snowflake Extractor Table configuration
// Completed
func resourceKeboolaExtractorTableRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Snowflake Extractor Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		if getExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}
	}

	var snowFlakeExtractor SnowFlakeExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&snowFlakeExtractor)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}

	for _, tableConfig := range snowFlakeExtractor.Configuration.Parameters.Tables {
		tableDetails := map[string]interface{}{
			"name":        tableConfig.Name,
			"table_id":    tableConfig.TableID,
			"incremental": tableConfig.Incremental,
			"enabled":     tableConfig.Enabled,
			"output":      tableConfig.OutputTable,
			"primary_key": tableConfig.PrimaryKey,
			"columns":     tableConfig.Columns,
			"schema":      tableConfig.Table.Schema,
			"table_name":  tableConfig.Table.TableName,
		}

		tables = append(tables, tableDetails)
	}

	d.Set("table", tables)

	return nil
}

// Update function to the resource Snowflake Extractor Table.
// It gets called when the keboola Provider calls it.
// Completed
func resourceKeboolaExtractorTableUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Snowflake Extractor Tables in Keboola.")

	tables := d.Get("table").(*schema.Set).List()

	mappedTables := make([]ExtractorTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := ExtractorTable{
			Name:        config["name"].(string),
			TableID:     config["table_id"].(int),
			Incremental: config["incremental"].(bool),
			OutputTable: config["output"].(string),
			Enabled:     config["enabled"].(bool),
		}

		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		if q := config["columns"]; q != nil {
			mappedTable.Columns = AsStringArray(q.([]interface{}))
		}

		sTable := SnowflakeExtractorDatabaseTable{
			Schema:    config["schema"].(string),
			TableName: config["table_name"].(string),
		}

		mappedTable.Table = sTable
		mappedTables = append(mappedTables, mappedTable)
	}

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}
	var snowFlakeExtractor SnowFlakeExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&snowFlakeExtractor)

	if err != nil {
		return err
	}

	snowFlakeExtractor.Configuration.Parameters.Tables = mappedTables

	snowflakeConfigJSON, err := json.Marshal(snowFlakeExtractor.Configuration)

	if err != nil {
		return err
	}

	updateSnowflakeForm := url.Values{}
	updateSnowflakeForm.Add("configuration", string(snowflakeConfigJSON))
	updateSnowflakeForm.Add("changeDescription", "Update Snowflake tables")

	updateSnowflakeBuffer := buffer.FromForm(updateSnowflakeForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()), updateSnowflakeBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaExtractorTableRead(d, meta)
}

// Delete function to the resource Snowflake Extractor Table.
// It gets called when the configuration is removed from terraform.
// Completed
func resourceKeboolaExtractorTableDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing Snowflake Extractor Tables in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var snowFlakeExtractor SnowFlakeExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&snowFlakeExtractor)

	if err != nil {
		return err
	}

	var emptyTables []ExtractorTable
	snowFlakeExtractor.Configuration.Parameters.Tables = emptyTables

	snowFlakeConfigJSON, err := json.Marshal(snowFlakeExtractor.Configuration)

	if err != nil {
		return err
	}

	clearSnowflakeTablesForm := url.Values{}
	clearSnowflakeTablesForm.Add("configuration", string(snowFlakeConfigJSON))
	clearSnowflakeTablesForm.Add("changeDescription", "Update Snowflake tables")

	clearSnowflakeTablesBuffer := buffer.FromForm(clearSnowflakeTablesForm)

	clearResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()), clearSnowflakeTablesBuffer)

	if hasErrors(err, clearResponse) {
		return extractError(err, clearResponse)
	}

	return nil
}
