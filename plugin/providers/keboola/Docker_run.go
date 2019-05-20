package keboola

import (
	"bytes"
	"fmt"
	"net/http"
)

// Allows the keboola platform to automatically run the COmponeent when uploaded via terraform to keboola.
func (c *KBCClient) PostToDockerRun(ComponentID string, ConfigID string) (*http.Response, error) {
	client := &http.Client{}

	body := []byte(fmt.Sprintf("{\n    \"config\": \"%s\",\n    \"component\": \"%s\",\n    \"mode\": \"run\"\n}", ConfigID, ComponentID))

	req, _ := http.NewRequest("POST", fmt.Sprintf("https://syrup.keboola.com/docker/%s/run", ComponentID), bytes.NewBuffer(body))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-StorageApi-Token", c.APIKey)

	return client.Do(req)
}
