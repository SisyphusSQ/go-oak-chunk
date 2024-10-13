package lag_checker

import (
	"testing"

	"go-oak-chunk/v2/conf"
	"go-oak-chunk/v2/mysql"
)

func Test_SlaveChecker(t *testing.T) {
	configPath := "../../conf/example.toml"
	config, err := conf.NewConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	config.PreCheck()

	masterClient, err := mysql.NewMysqlClient(config)
	if err != nil {
		t.Fatal(err)
	}

	s, err := NewSlaveChecker(masterClient, config)
	if err != nil {
		t.Fatal(err)
	}

	err = s.CheckLag()
	if err != nil {
		t.Fatal(err)
	}

	println(s.MaxLag)
}
