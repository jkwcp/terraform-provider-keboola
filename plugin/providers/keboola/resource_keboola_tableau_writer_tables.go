package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/hashcode"
	"github.com/hashicorp/terraform/helper/schema"
)

type TableauColumn struct {
	Name     string `json: "name"`
	DataType string `json:"datatype"`
	Title    string `json: "title"`
}

type TableauTable struct {
	ID      string                   `json:"tableId, omitempty"`
	Title   string                   `json:"title"`
	Export  bool                     `json:"export"`
	Columns map[string]TableauColumn `json:"columns"`
}

//Specifies the Create, Read, Update, and Delete functions for the Tableau Writer table
//Called from main.tf
func resourceKeboolaTableauWriterTables() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaTableauWriterTableCreate,
		Read:   resourceKeboolaTableauWriterTableRead,
		Update: resourceKeboolaTableauWriterTableUpdate,
		Delete: resourceKeboolaTableauWriterTableDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"title": {
				Type:     schema.TypeString,
				Required: true,
			},
			"export": {
				Type:     schema.TypeBool,
				Required: true,
			},
			"column": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"data_type": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
		},
	}
}

func mapColumns(d *schema.ResourceData) map[string]TableauColumn {
	columns := d.Get("column").(*schema.Set).List()
	mappedColumns := make(map[string]TableauColumn)

	for _, columnConfig := range columns {
		config := columnConfig.(map[string]interface{})

		mappedColumn := TableauColumn{
			Name:     config["name"].(string),
			DataType: config["data_type"].(string),
		}

		mappedColumns[mappedColumn.Name] = mappedColumn
	}
	return mappedColumns
}

//Creates a Tableau Writer table in Keboola Connection platform in the project
//Called from main.tf after resource_keboola_tableau_writer.go is called
//Currently incomplete, attempting to reconfigure based on the aws_redshift_writer_table.go file
func resourceKeboolaTableauWriterTableCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Tableau Writer Tables in Keboola.")

	client := meta.(*KBCClient)

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("title").(string)

	tableauTableConfig := TableauTable{
		Title:  tableID,
		Export: d.Get("export").(bool),
	}

	if d.Get("column") != nil {
		tableauTableConfig.Columns = mapColumns(d)
	}

	tableauTableJSON, err := json.Marshal(tableauTableConfig)

	if err != nil {
		return err
	}

	tableauTableBuffer := bytes.NewBuffer(tableauTableJSON)

	createResponse, err := client.PostToSyrup(fmt.Sprintf("tde-exporter/v2/%s/tables/%s", writerID, tableID), tableauTableBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	resourceKeboolaTableauWriterTableUpdate(d, meta)
	d.SetId(tableID)

	return resourceKeboolaTableauWriterTableRead(d, meta)
}

//Reads from a Tableau Writer table in Keboola Connection platform in the project and updates the table if the table configurations are different
//Called from main.tf after resource_keboola_tableau_writer.go is called
//Currently incomplete, attempting to reconfigure based on the aws_redshift_writer_table.go file
func resourceKeboolaTableauWriterTableRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Tableau Writer Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	writerID := d.Get("writer_id").(string)

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromSyrup(fmt.Sprintf("tde-exporter/v2/%s/tables/%s?include=columns", writerID, d.Id()))

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 400 || getResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResponse)
	}

	var tableauTable TableauTable

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&tableauTable)

	if err != nil {
		return err
	}

	columns := make([]interface{}, 0, len(tableauTable.Columns))

	for _, column := range tableauTable.Columns {
		columnDetails := map[string]interface{}{
			"data_type": column.DataType,
			"name":      column.Name,
			"title":     column.Title,
		}

		columns = append(columns, columnDetails)
	}

	if tableauTable.ID == d.Id() {
		d.Set("id", tableauTable.ID)
		d.Set("title", tableauTable.Title)
		d.Set("export", tableauTable.Export)
		d.Set("column", schema.NewSet(columnSetHash, columns))
	}

	return nil
}

func columnSetHash(v interface{}) int {
	var buffer bytes.Buffer
	m := v.(map[string]interface{})

	if v, ok := m["name"]; ok {
		buffer.WriteString(fmt.Sprintf("%s-", v.(string)))
	}

	return hashcode.String(buffer.String())
}

//Updates a Tableau Writer table in Keboola Connection platform in the project
//Called from main.tf after resource_keboola_tableau_writer.go is called
//Currently incomplete, attempting to reconfigure based on the GoodData_table.go file
func resourceKeboolaTableauWriterTableUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Tableau Writer Tables in Keboola.")

	client := meta.(*KBCClient)

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("title").(string)

	tableauTableConfig := TableauTable{
		Title:  tableID,
		Export: d.Get("export").(bool),
	}

	if d.Get("column") != nil {
		tableauTableConfig.Columns = mapColumns(d)
	}

	tableauTableJSON, err := json.Marshal(tableauTableConfig)

	if err != nil {
		return err
	}

	tableauTableBuffer := bytes.NewBuffer(tableauTableJSON)

	updateResponse, err := client.PostToSyrup(fmt.Sprintf("tde-exporter/v2/%s/tables/%s", writerID, tableID), tableauTableBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaTableauWriterTableRead(d, meta)
}

//Deletes a Tableau Writer table in Keboola Connection platform in the project
//Called from main.tf after resource_keboola_tableau_writer.go is called
//Currently incomplete, attempting to reconfigure based on the aws_redshift_writer_table.go file
func resourceKeboolaTableauWriterTableDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing Tableau Writer Tables in Keboola: %s", d.Id())

	writerID := d.Get("writer_id").(string)

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromSyrup(fmt.Sprintf("tde-exporter/v2/%s/tables/%s", writerID, d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
