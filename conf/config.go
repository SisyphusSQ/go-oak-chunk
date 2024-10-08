package conf

import (
	"fmt"
	"os"

	"github.com/pelletier/go-toml"
	"github.com/realcp1018/tinylog"

	"go-oak-chunk/v2/log"
	"go-oak-chunk/v2/vars"
)

type Config struct {
	ChunkSize           int64  `toml:"chunk_size"`
	ExecuteQuery        string `toml:"execute_query"`
	ForceChunkingColumn string `toml:"forced_chunking_column"`
	Host                string `toml:"host"`
	NoLogBin            bool   `toml:"no_log_bin"`
	User                string `toml:"user"`
	Password            string `toml:"password"`
	Port                int    `toml:"port"`
	PrintProgress       bool   `toml:"print_progress"`
	Sleep               int64  `toml:"sleep"`
	NoConsiderLag       bool   `toml:"no_consider_lag"`
	MaxLag              int64  `toml:"max_lag"`
	IncludeSlaves       string `toml:"include_slaves"`
	ExcludeSlaves       string `toml:"exclude_slaves"`

	//SkipLockTables      bool   `toml:"skip_lock_tables"`
	Database string `toml:"database"`
	TxnSize  int64  `toml:"txn_size"`
	Debug    bool   `toml:"debug_mode"`

	// 修正
	Correct int64 `toml:"correct"`
}

func NewConfig(configPath string) (*Config, error) {
	file, err := os.Open(configPath)
	defer file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to open config file, %s", err.Error())
	}
	decoder := toml.NewDecoder(file)
	c := new(Config)
	err = decoder.Decode(c)
	if err != nil {
		return nil, err
	}
	c.PreCheck()
	return c, nil
}

func (c *Config) PreCheck() {
	// config precheck
	if c.Debug {
		log.GlobalLogger.SetLevel(tinylog.LogLevel(vars.DEBUG))
		log.StreamLogger.SetLevel(tinylog.LogLevel(vars.DEBUG))
	} else {
		log.GlobalLogger.SetLevel(tinylog.LogLevel(vars.ERROR))
		log.StreamLogger.SetLevel(tinylog.LogLevel(vars.ERROR))
	}

	if c.ChunkSize < 0 {
		log.StreamLogger.Error("Chunk size must be nonnegative number. You can leave the default 1000 if unsure")
		os.Exit(1)
	}

	if c.ExecuteQuery == "" {
		log.StreamLogger.Error("Query to execute must be provided via -e or --execute")
		os.Exit(1)
	}

	if c.IncludeSlaves != "" && c.ExcludeSlaves != "" {
		log.StreamLogger.Error("--include-slaves and --exclude-slaves are mutually exclusive.")
		os.Exit(1)
	}
}
