package conf

import (
	"encoding/json"
	"testing"
)

func Test_config(t *testing.T) {
	configPath := "example.toml"
	config, err := NewConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	config.PreCheck()

	jsonString, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	println(string(jsonString))
}
