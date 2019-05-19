package keboola

type SSHTunnel struct {
	Enabled bool   `json:"enabled"`
	SSHHost string `json:"sshHost"`
	User    string `json:"user"`
	SSHPort string `json:"sshPort"`
	SSHKey  Keys   `json:"keys"`
}
type Keys struct {
	Public              string `json:"public"`
	PrivateKey          string `json:"private,omitempty"`
	PrivateKeyEncrypted string `json:"#private"`
}
