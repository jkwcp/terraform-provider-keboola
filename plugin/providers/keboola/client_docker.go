package keboola

import (
	"bytes"
	"fmt"
	"net/http"
)

const dockerURL = "https://docker-runner.keboola.com/docker/"

//PostToDocker posts a new object to the Keboola docker API.
func (c *KBCClient) PostToDockerEncrypt(componentID string, jsonpayload []byte, projectID string) (*http.Response, error) {
	client := &http.Client{}

	req, err := http.NewRequest("POST", fmt.Sprintf("https://docker-runner.keboola.com/docker/encrypt?componentId=%s&projectId=%s", componentID, projectID), bytes.NewBuffer(jsonpayload))

	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "text/plain")
	return client.Do(req)
}
func (c *KBCClient) PostToDockerCreateSSH(componentID string, jsonpayload []byte, projectID string) (*http.Response, error) {
	client := &http.Client{}

	req, err := http.NewRequest("POST", fmt.Sprintf("https://docker-runner.keboola.com/docker/encrypt?componentId=%s&projectId=%s", componentID, projectID), bytes.NewBuffer(jsonpayload))

	if err != nil {
		return nil, err
	}
	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("Content-Type", "text/plain")
	return client.Do(req)
}

func (c *KBCClient) PostToDockerAction(endpoint string, jsonpayload []byte) (*http.Response, error) {
	client := &http.Client{}

	req, err := http.NewRequest("POST", dockerURL+endpoint, bytes.NewBuffer(jsonpayload))

	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")
	return client.Do(req)
}

// Allows the keboola platform to automatically run the COmponeent when uploaded via terraform to keboola.
func (c *KBCClient) PostToDockerRun(ComponentID string, ConfigID string) (*http.Response, error) {
	client := &http.Client{}

	body := []byte(fmt.Sprintf("{\n    \"config\": \"%s\",\n    \"component\": \"%s\",\n    \"mode\": \"run\"\n}", ConfigID, ComponentID))

	req, _ := http.NewRequest("POST", fmt.Sprintf("https://syrup.keboola.com/docker/%s/run", ComponentID), bytes.NewBuffer(body))
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-StorageApi-Token", c.APIKey)

	return client.Do(req)
}
