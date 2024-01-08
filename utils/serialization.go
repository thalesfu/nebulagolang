package utils

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"log"
)

func MarshalJSON[T any](t T) string {
	content, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		log.Fatalf("序列化Json失败：%s", err)
		return ""
	}

	return string(content)
}

func UnmarshalJSON[T any](content string) (t *T, ok bool) {
	err := json.Unmarshal([]byte(content), &t)
	if err != nil {
		log.Fatalf("解析%T类型JSON失败：%s\nerror:\n%s", t, content, err)
		return nil, false
	}

	return t, true
}

func MarshalYaml[T any](t T) string {
	content, err := yaml.Marshal(t)
	if err != nil {
		log.Fatalf("序列化Yaml失败：%s", err)
		return ""
	}

	return string(content)
}

func UnmarshalYaml[T any](content string) (t *T, ok bool) {
	err := yaml.Unmarshal([]byte(content), &t)
	if err != nil {
		log.Fatalf("解析%T类型YAML失败：%s\nerror:\n%s", t, content, err)
		return nil, false
	}

	return t, true
}
