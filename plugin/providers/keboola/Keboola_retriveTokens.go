package keboola

import (
	"encoding/json"
	"io/ioutil"
	"strconv"
)

type Workspace struct {
	Owner struct {
		ID int `json:"id"`
	} `json:"owner"`
}

func ProjectID(client *KBCClient) (string, error) {

	resp, err := client.GetProjectID()
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	projectBody, _ := ioutil.ReadAll(resp.Body)
	workspace := Workspace{}
	id := workspace.Owner.ID
	json.Unmarshal(projectBody, &id)

	return strconv.Itoa(id), nil
}
