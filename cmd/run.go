package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"

	"github.com/spf13/cobra"

	"go-oak-chunk/v2/conf"
	"go-oak-chunk/v2/log"
	"go-oak-chunk/v2/task"
	"go-oak-chunk/v2/vars"
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
	//noLogBin            bool

	user          string
	password      string
	port          int
	printProgress bool
	sleep         int64
	//skipLockTables bool

	database      string
	txnSize       int64
	debug         bool
	noConsiderLag bool
	maxLag        int64
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "Start chunk dml",
	Long:    `Start chunk dml`,
	Example: fmt.Sprintf("%s run -c --config <config file>\n", vars.AppName),
	RunE: func(cmd *cobra.Command, args []string) error {
		log.StreamLogger.Debug("Go-oak-chunk start...")

		// parse configuration file or cmd line
		var (
			config *conf.Config
			err    error
		)
		if configPath != "" {
			config, err = conf.NewConfig(configPath)
			if err != nil {
				log.StreamLogger.Error(err.Error())
				return err
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
				//SkipLockTables: skipLockTables,
				Database:      database,
				Debug:         debug,
				NoConsiderLag: noConsiderLag,
				TxnSize:       txnSize,
				Correct:       50,
			}
			config.PreCheck()
		}

		f := StartCpuProfile()
		defer StopCpuProfile(f)

		// finish cpu perf profiling before ctrl-C/kill/kill -15
		ch := make(chan os.Signal, 5)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			for {
				sig := <-ch
				switch sig {
				case syscall.SIGINT, syscall.SIGTERM:
					log.StreamLogger.Debug("Terminating process, will finish cpu pprof before exit(if specified)...")
					StopCpuProfile(f)
					os.Exit(1)
				default:
				}
			}
		}()

		// run task
		err = task.RunTask(config)
		if err != nil {
			log.StreamLogger.Error(err.Error())
			return err
		}

		// do memory profiling before exit
		MemProfile()
		return nil
	},
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
	//runCmd.Flags().BoolVar(&noLogBin, "no-log-bin", false, "Do not log to binary log (actions will not replicate). This may be useful if the slave already finds it hard to replicate behind master. The utility may be spawned manually on slave machines, therefore utilizing more than one CPU core on those machines, making replication process faster due to parallelism.")
	runCmd.Flags().BoolVar(&printProgress, "print-progress", false, "Show number of affected rows during utility runtime")
	runCmd.Flags().Int64Var(&sleep, "sleep", 0, "Number of seconds to sleep between chunks.")
	runCmd.Flags().BoolVar(&noConsiderLag, "noConsiderLag", false, "If true: sleep value will not be overshoot\nfalse: if slave lag is very high, sleep will be overshoot")
	//runCmd.Flags().BoolVar(&skipLockTables, "skip-lock-tables", false, "Do not issue a LOCK TABLES READ. May be required when using queries within --start-with or --end-with")
	runCmd.Flags().StringVarP(&database, "database", "d", "", "Database name (required unless table is fully qualified)")
	runCmd.Flags().Int64Var(&txnSize, "txn-size", 1000, "Number of rows per transaction.")
	runCmd.Flags().Int64Var(&maxLag, "max-lag", 0, "Pause chunk dml if the slave reach Threshold.")
	runCmd.Flags().BoolVar(&debug, "debug", false, "If debug_mode is true, print debug logs")
	rootCmd.AddCommand(runCmd)
}

func StartCpuProfile() *os.File {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.StreamLogger.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.StreamLogger.Fatal("could not start CPU profile: ", err)
		}
		log.StreamLogger.Info("cpu pprof start ...")
		return f
	}
	return nil
}

func StopCpuProfile(f *os.File) {
	if f != nil {
		pprof.StopCPUProfile()
		f.Close()
		log.StreamLogger.Info("cpu pprof stopped [file=%s]!", cpuprofile)
		return
	}
}

func MemProfile() {
	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.StreamLogger.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.StreamLogger.Fatal("could not write memory profile: ", err)
		}
		log.StreamLogger.Info("mem pprof done [file=%s]!", memprofile)
	}
}
