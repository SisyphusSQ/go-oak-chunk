package mysql

import (
	"database/sql"
	"strconv"

	_ "github.com/go-sql-driver/mysql"

	"go-oak-chunk/v2/conf"
)

func NewMysqlClient(t *conf.Config) (*sql.DB, error) {
	dsn := t.User + ":" + t.Password + "@(" + t.Host + ":" + strconv.Itoa(t.Port) + ")/" + t.Database
	dsn += "?checkConnLiveness=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	return db, nil
}

func NewMysqlClientForSlave(t *conf.Config, host string) (*sql.DB, error) {
	dsn := t.User + ":" + t.Password + "@(" + host + ":" + strconv.Itoa(t.Port) + ")/" + t.Database
	dsn += "?checkConnLiveness=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(5)
	return db, nil
}

func CheckVersion(client *sql.DB) (version string, err error) {
	err = client.QueryRow("select @@version").Scan(&version)
	if err != nil {
		return "", err
	}
	return version, nil
}
