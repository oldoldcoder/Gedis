package config

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

var Banner = `
 ██████╗ ███████╗██████╗ ██╗███████╗
██╔════╝ ██╔════╝██╔══██╗██║██╔════╝
██║  ███╗█████╗  ██║  ██║██║███████╗
██║   ██║██╔══╝  ██║  ██║██║╚════██║
╚██████╔╝███████╗██████╔╝██║███████║
 ╚═════╝ ╚══════╝╚═════╝ ╚═╝╚══════╝
`
var DefaultProperties = &ServerProperties{
	Bind:       "0.0.0.0",
	Port:       6399,
	MaxClients: 1000,
}

type ServerProperties struct {
	Bind        string   `yaml:"bind"`
	Port        int      `yaml:"port"`
	MaxClients  int      `yaml:"maxclients"`
	RequirePass string   `yaml:"requirepass"`
	Databases   int      `yaml:"databases"`
	Peers       []string `yaml:"peers"`
}

var Properties *ServerProperties

func init() {
	Properties = &ServerProperties{
		Bind: "127.0.0.1",
		Port: 6379,
	}
}

func SetupConfig(configFileName string) {
	file, err := ioutil.ReadFile(configFileName)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(file, Properties)
	if err != nil {
		panic(err)
	}

}
