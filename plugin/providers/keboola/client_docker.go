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
