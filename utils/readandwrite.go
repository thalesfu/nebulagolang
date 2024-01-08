package utils

import (
	"log"
	"os"
	"path/filepath"
)

func LoadContent(path string) (string, bool) {
	if _, err := os.Stat(path); err != nil {
		return "", false
	}

	bytes, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
		return "", false
	}

	return string(bytes), true
}

func WriteContent(filePath string, content string) {
	dirPath := filepath.Dir(filePath)

	// 创建目录，包括任何必需的父目录，权限设置为 0755
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		log.Fatal(err)
	}

	// 创建新的文件
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("创建文件失败：%s\nerror:%v", filePath, err)
	}

	defer func(f *os.File) {
		log.Printf("[文件保存]写入并保存文件: %s", f.Name())
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file)

	// 将内容写入文件
	if _, err := file.WriteString(content); err != nil {
		log.Fatal(err)
	}
}
