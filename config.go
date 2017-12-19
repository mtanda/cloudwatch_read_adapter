package main

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type Config struct {
	Targets []Target `yaml:"targets"`
}

type Target struct {
	Index   IndexConfig   `yaml:"index"`
	Archive ArchiveConfig `yaml:"archive"`
}

type IndexConfig struct {
	Region    []*string `yaml:"region"`
	Namespace []*string `yaml:"namespace"`
	Retention string    `yaml:"retention"`
}

type ArchiveConfig struct {
	Region    []*string `yaml:"region"`
	Namespace []*string `yaml:"namespace"`
	Retention string    `yaml:"retention"`
}

func LoadConfig(configFile string) (*Config, error) {
	buf, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	var cfg Config
	err = yaml.Unmarshal(buf, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
