package nebulagolang

import "github.com/thalesfu/nebulagolang/utils"

type Account struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func LoadAccount() (*Account, bool) {
	content, ok := utils.LoadContent("nebula-account.yaml")
	if !ok {
		return nil, false
	}

	return utils.UnmarshalYaml[Account](content)
}
