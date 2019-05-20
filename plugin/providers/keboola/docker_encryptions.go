package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	req.Header.Set("X-Storageapi-Token", c.APIKey)
	req.Header.Add("Content-Type", "text/plain")
	return client.Do(req)
}

func (c *KBCClient) PostToDockerCreateSSH() (key Keys, err error) {

	body := []byte("{\n  \"configData\": {\n    \"parameters\": {}\n  }\n}") //\n \"public\":{}
	req, err := http.NewRequest("POST", "https://docker-runner.keboola.com/docker/keboola.ssh-keygen-v2/action/generate", bytes.NewBuffer(body))

	req.Header.Set("X-Storageapi-Token", c.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)

	defer resp.Body.Close()

	resp_body, _ := ioutil.ReadAll(resp.Body)
	key = Keys{}
	json.Unmarshal(resp_body, &key)
	return key, err
}
func encyrptPassword(componentID string, value string, client *KBCClient) (str_body string, err error) {
	body := []byte(value)
	projectID, err := ProjectID(client)

	createResponseConfig, err := client.PostToDockerEncrypt(componentID, body, projectID)
	defer createResponseConfig.Body.Close()
	resp_body, err := ioutil.ReadAll(createResponseConfig.Body)

	if hasErrors(err, createResponseConfig) {
		return "", err
	}
	str_body = string(resp_body)
	return str_body, nil
}
