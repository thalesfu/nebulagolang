package nebulagolang

import (
	"github.com/thalesfu/nebulagolang/utils"
)

type Account struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

func LoadAccount() (*Account, bool) {

	// 构建完整的文件路径
	filePath := "nebula-account.yaml"

	content, ok := utils.LoadContent(filePath)
	if ok {
		return utils.UnmarshalYaml[Account](content)
	}

	return &Account{
		Host:     "127.0.0.1",
		Port:     9669,
		Username: "root",
	}, true
}
