package keboola

import (
	"bytes"
	"fmt"
	"net/http"
)

const dockerURL = "https://docker-runner.keboola.com/docker/encrypt?"

//PostToDocker posts a new object to the Keboola docker API.
func (c *KBCClient) PostToDockerEncrypt(componentID string, projectID string, jsonpayload []byte) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", fmt.Sprintf("https://docker-runner.keboola.com/docker/encrypt?componentId=%s&projectId=%s", componentID, projectID), bytes.NewBuffer(jsonpayload))

	if err != nil {
		return nil, err
	}

	//req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("Content-Type", "text/plain")
	return client.Do(req)
}
