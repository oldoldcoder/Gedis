package main

import (
	"fmt"
	"gedis/config"
	"gedis/lib/logger"
	"gedis/redis/server"
	"gedis/tcp"
	"os"
)

func fileExists(fileName string) bool {
	info, err := os.Stat(fileName)
	return err == nil && !info.IsDir()
}

func main() {
	print(config.Banner)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "gedis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	// os.env获取环境变量
	configFileName := os.Getenv("CONFIG")
	if configFileName == "" {
		if fileExists("gedis.yaml") {
			config.SetupConfig("gedis.yaml")
		} else {
			config.Properties = config.DefaultProperties
		}
	} else {
		config.SetupConfig(configFileName)
	}

	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, server.MakeHandler())

	if err != nil {
		logger.Error(err)
	}
}
