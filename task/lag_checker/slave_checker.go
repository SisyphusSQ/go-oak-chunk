package lag_checker

import (
	"database/sql"
	"strconv"
	"strings"

	"go-oak-chunk/v2/conf"
	"go-oak-chunk/v2/log"
	"go-oak-chunk/v2/mysql"
	"go-oak-chunk/v2/utils/string_utils"
)

type SlaveChecker struct {
	MaxLag int64
	Slaves []*slaveInfo
}

type slaveInfo struct {
	MysqlClient *sql.DB
	host        string
	lagSql      string
	canSkip     bool
}

func NewSlaveChecker(masterClient *sql.DB, config *conf.Config) (*SlaveChecker, error) {
	var (
		hosts         = make([]mysql.SlaveHost, 0)
		mysqlClients  = make([]*sql.DB, 0)
		slaves        = make([]*slaveInfo, 0)
		includeSlaves = strings.Split(config.IncludeSlaves, ",")
		excludeSlaves = strings.Split(config.ExcludeSlaves, ",")
	)
	showSlaveSql, err := getCheckSql(masterClient, "master")
	if err != nil {
		return nil, err
	}

	rows, err := masterClient.Query(showSlaveSql)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var h mysql.SlaveHost
		err = rows.Scan(&h.ServerId, &h.Host, &h.Port, &h.MasterId, &h.SlaveUUID)
		if err != nil {
			return nil, err
		}

		hosts = append(hosts, h)
	}

	for _, host := range hosts {
		log.StreamLogger.Debug("slave host: [%s]", host.Host)
		if config.IncludeSlaves != "" {
			if !string_utils.ContainsAny(host.Host, includeSlaves) {
				continue
			}
		} else if config.ExcludeSlaves != "" {
			if string_utils.ContainsAny(host.Host, excludeSlaves) {
				continue
			}
		}
		log.StreamLogger.Debug("Prepare to check slave lag, host: [%s]", host.Host)

		client, err := mysql.NewMysqlClientForSlave(config, host.Host)
		if err != nil {
			log.StreamLogger.Debug("Slave host can't be created, host: [%s]", host.Host)
			continue
		}

		lagSql, err := getCheckSql(client, "slave")
		if err != nil {
			log.StreamLogger.Debug("Can't get slave lag check sql, host: [%s]", host.Host)
			continue
		}

		slave := &slaveInfo{
			host:        host.Host,
			MysqlClient: client,
			lagSql:      lagSql,
		}
		slaves = append(slaves, slave)
		mysqlClients = append(mysqlClients, client)
	}

	slaveChecker := &SlaveChecker{
		Slaves: slaves,
	}
	return slaveChecker, nil
}

func (s *SlaveChecker) CheckLag() error {
	var maxLag int64
	for _, sl := range s.Slaves {
		if sl.canSkip {
			continue
		}

		// 默认不是多源同步
		slaveLag, err := s.getSlaveLag(sl.MysqlClient, sl.lagSql)
		if err != nil {
			// 如果此处取Seconds_Behind_Master报错了。则默认该从库已坏，并将其从延迟检测中摘除
			sl.canSkip = true
			log.StreamLogger.Warn("SlaveHost[%s], fetch slave lag got err: %v", sl.host, err)
			continue
		}

		log.StreamLogger.Debug("SlaveHost[%s], Seconds_Behind_Master: %d", sl.host, slaveLag)
		if slaveLag > maxLag {
			maxLag = slaveLag
		}
	}

	s.MaxLag = maxLag
	log.StreamLogger.Debug("MaxLag Seconds_Behind_Master: %d", maxLag)
	return nil
}

func (s *SlaveChecker) Close() {
	for _, sl := range s.Slaves {
		sl.MysqlClient.Close()
	}
}

func (s *SlaveChecker) getSlaveLag(client *sql.DB, lagSql string) (slaveLag int64, err error) {
	slaveStatusRows, err := client.Query(lagSql)
	if err != nil {
		return 0, err
	}

	slaveCols, err := slaveStatusRows.Columns()
	if err != nil {
		return 0, err
	}

	for slaveStatusRows.Next() {
		scanArgs := make([]interface{}, len(slaveCols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}

		if err = slaveStatusRows.Scan(scanArgs...); err != nil {
			return 0, err
		}

		slaveLag, err = mysql.ColumnValueInt64(scanArgs, slaveCols, "Seconds_Behind_Master")
		if err != nil {
			return 0, err
		}
	}
	return slaveLag, nil
}

func getCheckSql(client *sql.DB, either string) (string, error) {
	// 首先确定MySQL的版本
	// 8.0.21以下版本用 SHOW SLAVE STATUS
	// 8.0.22 以及8.1.x 8.2.x ...用 SHOW REPLICA STATUS
	version, err := mysql.CheckVersion(client)
	if err != nil {
		return "", err
	}

	ver1, _ := strconv.ParseInt(strings.Split(version, ".")[0], 10, 64)
	ver2, _ := strconv.ParseInt(strings.Split(version, ".")[1], 10, 64)
	subStr := strings.Split(strings.Split(version, ".")[2], "-")[0]
	ver3, _ := strconv.ParseInt(subStr, 10, 64)

	if either == "master" {
		if (ver1 >= 8 && ver3 >= 21) || (ver1 >= 8 && ver2 > 0) {
			return "SHOW REPLICA HOSTS", nil
		} else {
			return "SHOW SLAVE HOSTS", nil
		}
	} else {
		// either == "slave"
		if (ver1 >= 8 && ver3 >= 21) || (ver1 >= 8 && ver2 > 0) {
			return "SHOW REPLICA STATUS", nil
		} else {
			return "SHOW SLAVE STATUS", nil
		}
	}
}
