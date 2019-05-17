package keboola

import (
	"bytes"
	"fmt"
	"net/http"
)

const dockerURL = "https://docker-runner.keboola.com/docker/encrypt?"

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

/*
func (c *KBCClient) PostToDockerCreateSSH(componentID string, jsonpayload []byte, projectID string) (*http.Response, error) {
	client := &http.Client{}

	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("POST", "https://docker-runner.keboola.com/docker/keboola.ssh-keygen-v2/action/generate", body)
	if err != nil {
		// handle err
	}
	req.Header.Set("X-Storageapi-Token", "{STORAGE_API_TOKEN}")
	req.Header.Set("Content-Type", "application/json")

	return client.Do(req)
}
*/
