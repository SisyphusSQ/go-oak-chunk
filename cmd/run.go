package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/spf13/cobra"

	oak "github.com/SisyphusSQ/go-oak-chunk/v3"
	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/log"
	"github.com/SisyphusSQ/go-oak-chunk/v3/task"
	"github.com/SisyphusSQ/go-oak-chunk/v3/vars"
)

var (
	configPath string
	cpuprofile string
	memprofile string

	chunkSize           int64
	executeQuery        string
	forceChunkingColumn string
	host                string
	includeSlaves       string
	excludeSlaves       string
	noSlaves            bool
	//noLogBin            bool

	user          string
	password      string
	port          int
	printProgress bool
	noProgressBar bool
	sleep         int64
	//skipLockTables bool

	database      string
	txnSize       int64
	debug         bool
	noConsiderLag bool
	maxLag        int64
	rowsPerSec    int64

	// P2 flags
	selectIndex        string
	selectOrderBy      string
	selectCursor       bool
	maxRows            int64
	maxDurationMs      int64
	dryRun             bool
	preflightThreshold int64
	autoConfirm        bool

	// P3 flags
	partitionConcurrency int

	// P4 flags
	tidbRowID bool
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "Start chunk dml",
	Long:    `Start chunk dml`,
	Example: fmt.Sprintf("%s run -c --config <config file>\n", vars.AppName),
	RunE: func(cmd *cobra.Command, args []string) error {
		config, err := loadRunConfig()
		if err != nil {
			return err
		}

		if err = log.New(config.Debug, log.OutputStderr); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
			return err
		}
		defer log.Logger.Sync()
		log.Logger.Debugf("go-oak-chunk starting [host=%s:%d, database=%s, execute=%s]",
			config.Host, config.Port, config.Database, config.ExecuteQuery)

		f, err := StartCpuProfile()
		if err != nil {
			log.Logger.Errorf("failed to start CPU profile [cpuprofile=%s]: %v", cpuprofile, err)
			return err
		}
		defer StopCpuProfile(f)

		executor, err := oak.NewExecutor(config)
		if err != nil {
			log.Logger.Errorf("failed to create executor [host=%s:%d, database=%s]: %v", config.Host, config.Port, config.Database, err)
			return err
		}

		runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		// run task
		log.Logger.Infof("task started [host=%s:%d, database=%s]", config.Host, config.Port, config.Database)
		err = executor.Run(runCtx)
		if err != nil {
			if errors.Is(err, task.ErrExecutionStopped) {
				log.Logger.Info("task stopped by signal (SIGINT/SIGTERM)")
				return nil
			}
			log.Logger.Errorf("task execution failed [execute=%s]: %v", config.ExecuteQuery, err)
			return err
		}
		log.Logger.Info("task completed successfully")

		// do memory profiling before exit
		if err = MemProfile(); err != nil {
			log.Logger.Errorf("failed to write memory profile [memprofile=%s]: %v", memprofile, err)
			return err
		}
		return nil
	},
}

func loadRunConfig() (*conf.Config, error) {
	// parse configuration file or cmd line
	var (
		config *conf.Config
		err    error
	)
	if configPath != "" {
		config, err = conf.NewConfig(configPath)
		if err != nil {
			log.Logger.Errorf("failed to load config [path=%s]: %v", configPath, err)
			return nil, err
		}
	} else {
		config = &conf.Config{
			ChunkSize:           chunkSize,
			ExecuteQuery:        executeQuery,
			ForceChunkingColumn: forceChunkingColumn,
			Host:                host,
			//NoLogBin:            noLogBin,
			User:          user,
			Password:      password,
			Port:          port,
			PrintProgress: printProgress,
			Sleep:         sleep,
			MaxLag:        maxLag,
			IncludeSlaves: includeSlaves,
			ExcludeSlaves: excludeSlaves,
			NoSlaves:      noSlaves,
			//SkipLockTables: skipLockTables,
			Database:      database,
			Debug:         debug,
			NoConsiderLag: noConsiderLag,
			TxnSize:       txnSize,
			RowsPerSec:    rowsPerSec,
			Correct:       50,

			SelectIndex:        selectIndex,
			SelectOrderBy:      selectOrderBy,
			SelectCursor:       selectCursor,
			MaxRows:            maxRows,
			MaxDuration:        maxDurationMs,
			DryRun:             dryRun,
			PreflightThreshold: preflightThreshold,
			AutoConfirm:        autoConfirm,

			PartitionConcurrency: partitionConcurrency,
			TiDBRowID:            tidbRowID,
		}
		if err = config.PreCheck(); err != nil {
			log.Logger.Errorf("config precheck failed [host=%s, database=%s]: %v", host, database, err)
			return nil, err
		}
	}
	// This output override also applies when business options come from TOML.
	if noProgressBar {
		config.PrintProgress = false
	}
	return config, nil
}

func initRun() {
	runCmd.Flags().StringVarP(&configPath, "config", "c", "", "config file path")
	runCmd.Flags().StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to `file`")
	runCmd.Flags().StringVar(&memprofile, "memprofile", "", "write memory profile to `file`")
	runCmd.Flags().Int64Var(&chunkSize, "chunk-size", 1000, "Number of rows to act on in chunks.\nZero(0) means all rows updated in one operation.\nOne(1) means update/delete one row everytime.\nThe lower the number, the shorter any locks are held, but the more operations required and the more total running time.")
	runCmd.Flags().StringVarP(&executeQuery, "execute", "e", "", "Query to execute, which must contain where clause")
	runCmd.Flags().StringVar(&forceChunkingColumn, "force-chunking-column", "", "Columns to chunk by. Format: for single column keys, or column1_name,column2_name,...")
	runCmd.Flags().StringVarP(&host, "host", "H", "localhost", "MySQL host")
	runCmd.Flags().IntVarP(&port, "port", "P", 3306, "TCP/IP port")
	runCmd.Flags().StringVarP(&user, "user", "u", "root", "MySQL user")
	runCmd.Flags().StringVarP(&password, "password", "p", "", "MySQL password")
	runCmd.Flags().StringVar(&includeSlaves, "include-slaves", "", "which slaves should be include, include_slaves and exclude_slaves are mutually exclusive.\nex: ip or ip1,ip2,... without port")
	runCmd.Flags().StringVar(&excludeSlaves, "exclude-slaves", "", "which slaves should be include, include_slaves and exclude_slaves are mutually exclusive.\nex: ip or ip1,ip2,... without port")
	runCmd.Flags().BoolVar(&noSlaves, "no-slaves", false, "If true: don't calculate lags on slaves")
	//runCmd.Flags().BoolVar(&noLogBin, "no-log-bin", false, "Do not log to binary log (actions will not replicate). This may be useful if the slave already finds it hard to replicate behind master. The utility may be spawned manually on slave machines, therefore utilizing more than one CPU core on those machines, making replication process faster due to parallelism.")
	runCmd.Flags().BoolVar(&printProgress, "print-progress", false, "Show number of affected rows during utility runtime")
	runCmd.Flags().BoolVar(&noProgressBar, "no-progress-bar", false, "Use log-only output without terminal progress display; overrides --print-progress and config print_progress")
	runCmd.Flags().Int64Var(&sleep, "sleep", 0, "Number of milliseconds to sleep between chunks.")
	runCmd.Flags().BoolVar(&noConsiderLag, "noConsiderLag", false, "If true: sleep value will not be overshoot\nfalse: if slave lag is very high, sleep will be overshoot")
	//runCmd.Flags().BoolVar(&skipLockTables, "skip-lock-tables", false, "Do not issue a LOCK TABLES READ. May be required when using queries within --start-with or --end-with")
	runCmd.Flags().StringVarP(&database, "database", "d", "", "Database name. Optional when --execute uses schema.table; if both are set, they must match exactly, including letter case")
	runCmd.Flags().Int64Var(&txnSize, "txn-size", 1000, "Number of rows per transaction.")
	runCmd.Flags().Int64Var(&maxLag, "max-lag", 0, "Pause chunk dml if the slave reach Threshold.")
	runCmd.Flags().BoolVar(&debug, "debug", false, "If debug_mode is true, print debug logs")
	runCmd.Flags().Int64Var(&rowsPerSec, "rows-per-sec", 0, "Cap rows acted on per second (0=unlimited)")

	// P2: OB covering-index fast-path (DELETE only) and shared guardrails.
	runCmd.Flags().StringVar(&selectIndex, "select-index", "", "FORCE INDEX name for the two-phase candidate SELECT (covering index strategy)")
	runCmd.Flags().StringVar(&selectOrderBy, "select-order-by", "", "Order columns for the two-phase candidate SELECT (comma-separated). Enables covering-index fast-path (DELETE only)")
	runCmd.Flags().BoolVar(&selectCursor, "select-cursor", false, "Use a cursor to advance the candidate SELECT, avoiding a re-scan from the start")
	runCmd.Flags().Int64Var(&maxRows, "max-rows", 0, "Stop after acting on this many rows (0=unlimited)")
	runCmd.Flags().Int64Var(&maxDurationMs, "max-duration-ms", 0, "Stop after this many milliseconds (0=unlimited)")
	runCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print sample SQL without executing")
	runCmd.Flags().Int64Var(&preflightThreshold, "preflight-threshold", 0, "EXPLAIN large-table confirmation threshold (0=default 100000)")
	runCmd.Flags().BoolVar(&autoConfirm, "yes", false, "Skip the large-table confirmation prompt")

	// P3: OceanBase partition-parallel covering DELETE.
	runCmd.Flags().IntVar(&partitionConcurrency, "partition-concurrency", 0,
		"OceanBase only: run the covering-index DELETE across table partitions with this many parallel workers (0/1=off)")

	// P4: TiDB _tidb_rowid chunked DELETE.
	runCmd.Flags().BoolVar(&tidbRowID, "tidb-rowid", false,
		"TiDB only: chunk DELETE by the hidden _tidb_rowid handle (NONCLUSTERED tables, no PK/UK required)")

	rootCmd.AddCommand(runCmd)
}

func StartCpuProfile() (*os.File, error) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			return nil, fmt.Errorf("could not create CPU profile: %w", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return nil, fmt.Errorf("could not start CPU profile: %w", err)
		}
		log.Logger.Infof("cpu pprof started [file=%s]", cpuprofile)
		return f, nil
	}
	return nil, nil
}

func StopCpuProfile(f *os.File) {
	if f != nil {
		pprof.StopCPUProfile()
		f.Close()
		log.Logger.Infof("cpu pprof stopped [file=%s]", cpuprofile)
		return
	}
}

func MemProfile() error {
	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			return fmt.Errorf("could not create memory profile: %w", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			return fmt.Errorf("could not write memory profile: %w", err)
		}
		log.Logger.Infof("mem pprof done [file=%s]", memprofile)
	}
	return nil
}
