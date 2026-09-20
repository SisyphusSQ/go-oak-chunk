package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/gdamore/tcell"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/internal/preflight"
	"github.com/SisyphusSQ/go-oak-chunk/v3/log"
	"github.com/SisyphusSQ/go-oak-chunk/v3/mysql"
	"github.com/SisyphusSQ/go-oak-chunk/v3/task/lag_checker"
	"github.com/SisyphusSQ/go-oak-chunk/v3/vars"
)

var ErrExecutionStopped = errors.New("execution stopped")

type ProgressSnapshot struct {
	RowAffects   int64
	ElapsedTime  time.Duration
	CurrentSleep int64
	MaxSlaveLag  int64
	IsFinished   bool
}

type ProgressCallback func(status *ProgressSnapshot)

type RunOptions struct {
	RateLimiter      *RateLimiter
	ProgressInterval time.Duration
	ProgressCallback ProgressCallback
}

func RunTask(config *conf.Config) error {
	if err := config.PreCheck(); err != nil {
		return err
	}

	writer, err := mysql.NewWriter(config)
	if err != nil {
		return err
	}

	opts := RunOptions{}
	if config.PrintProgress {
		opts.ProgressInterval = 3 * time.Second
	}

	return Execute(context.Background(), config, writer, opts)
}

func Execute(ctx context.Context, config *conf.Config, writer *mysql.Writer, opts RunOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if config == nil {
		return fmt.Errorf("config is nil")
	}
	if writer == nil {
		return fmt.Errorf("writer is nil")
	}

	// print json of config
	configJson, err := json.Marshal(&config)
	if err == nil {
		log.Logger.Debugf("config json: %s", string(configJson))
	}

	var (
		wg                    sync.WaitGroup
		runCtx, cancel        = context.WithCancel(ctx)
		deferCancelOnce       sync.Once
		cleanupOnce           sync.Once
		printProgressDoneChan = make(chan struct{}, 1)
		bucketNum             = make(chan int64, 1000)
	)
	defer func() {
		deferCancelOnce.Do(cancel)
	}()

	rateLimiter := opts.RateLimiter
	if rateLimiter == nil {
		rateLimiter = NewRateLimiterFromConfig(config)
	}

	rowsLimiter := NewRowsLimiterFromConfig(config)

	progressInterval := opts.ProgressInterval
	if progressInterval <= 0 {
		progressInterval = 3 * time.Second
	}

	sl, err := lag_checker.NewSlaveCheckerWithContext(runCtx, writer.MysqlClient, config)
	if err != nil {
		log.Logger.Errorf("create SlaveChecker goroutine is failed, err: %v", err)
		sl = nil
	}

	cleanup := func() {
		cleanupOnce.Do(func() {
			if sl != nil {
				sl.Close()
			}
			if writer.MysqlClient != nil {
				_ = writer.MysqlClient.Close()
			}
		})
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		getStopTime(runCtx, sl, bucketNum, rateLimiter, writer)
		log.Logger.Debug("getStopTime goroutine is finished")
	}()

	// Preflight runs before the first chunk. Skipped under dry-run (no DB work).
	if !config.DryRun {
		if err := runPreflight(runCtx, config, writer); err != nil {
			deferCancelOnce.Do(cancel)
			cleanup()
			return err
		}
	}

	strategy := selectStrategy(config, writer)
	log.Logger.Infof("selected strategy: %s", strategy.Name())

	executeErrChan := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		executeErrChan <- strategy.Run(runCtx, mysql.RunParams{
			Bucket:      rateLimiter.Bucket(),
			BucketNum:   bucketNum,
			RowsLimiter: rowsLimiter,
		})
	}()

	tasksDoneChan := make(chan struct{}, 1)
	go func() {
		wg.Wait()
		tasksDoneChan <- struct{}{}
	}()

	progressEnabled := false
	if opts.ProgressCallback != nil {
		progressEnabled = true
		go runProgressCallback(runCtx, rateLimiter, writer, progressInterval, opts.ProgressCallback, printProgressDoneChan)
	} else if config.PrintProgress {
		progressEnabled = true
		go PrintProgress(config, writer, progressInterval, runCtx, printProgressDoneChan)
	}

	waitProgressDone := func() {
		if progressEnabled {
			select {
			case <-printProgressDoneChan:
			case <-time.After(3 * time.Second):
				log.Logger.Warn("wait progress callback shutdown timeout")
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			deferCancelOnce.Do(cancel)
			cleanup()
			waitProgressDone()
			if errors.Is(ctx.Err(), context.Canceled) {
				return ErrExecutionStopped
			}
			return ctx.Err()
		case execErr := <-executeErrChan:
			if execErr != nil {
				execErr = normalizeStopError(execErr)
				deferCancelOnce.Do(cancel)
				cleanup()
				waitProgressDone()
				return execErr
			}
			executeErrChan = nil
		case <-tasksDoneChan:
			deferCancelOnce.Do(cancel)
			cleanup()
			waitProgressDone()
			return nil
		}
	}
}

// selectStrategy 根据配置和 writer 元信息选择执行策略。
//
// 决策树(优先级从高到低):
//  1. config.TiDBRowID -> TiDB _tidb_rowid 分块 DELETE(NONCLUSTERED 表, 无需 PK/UK)。
//     适用性(数据源/聚簇性) 由 TiDBRowIDStrategy.Run 在运行时校验。
//  2. config.PartitionConcurrency>1 + OceanBase + 覆盖快路径 -> OB 分区并行 DELETE。
//  3. config.SelectOrderBy 非空 -> 覆盖索引两阶段 fast-path(DELETE only)。
//     SqlType 与依赖关系已在 config.PreCheck 校验。
//  4. 否则 -> 默认范围分块 RangeStrategy(行为零变化)。
func selectStrategy(config *conf.Config, writer *mysql.Writer) mysql.ChunkStrategy {
	// P4: TiDB _tidb_rowid chunked DELETE. Explicit opt-in via --tidb-rowid;
	// the DataSource==tidb / NONCLUSTERED checks need a DB connection and run in
	// TiDBRowIDStrategy.Run, keeping selectStrategy connection-free.
	if config.TiDBRowID {
		return mysql.NewTiDBRowIDStrategy(writer, &mysql.TiDBRowIDOptions{
			DryRun:      config.DryRun,
			MaxRows:     config.MaxRows,
			MaxDuration: time.Duration(config.MaxDuration) * time.Millisecond,
		})
	}

	// P3: OceanBase partition-parallel covering DELETE. Requires the covering
	// fast-path (validated in PreCheck) and an OceanBase data source. Whether the
	// table is actually partitioned is verified at runtime by discoverPartitions.
	if config.PartitionConcurrency > 1 &&
		writer.DataSource() == "oceanbase" &&
		strings.TrimSpace(config.SelectOrderBy) != "" {
		return mysql.NewOBPartitionStrategy(writer, &mysql.OBPartitionOptions{
			SelectIndex:   config.SelectIndex,
			SelectOrderBy: config.SelectOrderBy,
			SelectCursor:  config.SelectCursor,
			DryRun:        config.DryRun,
			MaxRows:       config.MaxRows,
			MaxDuration:   time.Duration(config.MaxDuration) * time.Millisecond,
			Concurrency:   config.PartitionConcurrency,
		})
	}
	if strings.TrimSpace(config.SelectOrderBy) != "" {
		return mysql.NewOBCoveringStrategy(writer, &mysql.OBCoveringOptions{
			SelectIndex:   config.SelectIndex,
			SelectOrderBy: config.SelectOrderBy,
			SelectCursor:  config.SelectCursor,
			DryRun:        config.DryRun,
			MaxRows:       config.MaxRows,
			MaxDuration:   time.Duration(config.MaxDuration) * time.Millisecond,
		})
	}
	return mysql.NewRangeStrategy(writer)
}

// runPreflight estimates the affected row count via EXPLAIN before launching the
// strategy. EXPLAIN failures are non-fatal (logged, then continue). When the
// estimate reaches the threshold and confirmation is not auto-granted, it reads
// an interactive yes/no on stdin (CLI); SDK callers pass AutoConfirm.
func runPreflight(ctx context.Context, config *conf.Config, writer *mysql.Writer) error {
	threshold := config.PreflightThreshold
	if threshold <= 0 {
		threshold = preflight.DefaultLargeTableThreshold
	}

	table := mysql.QualifiedTableName(writer.Database, writer.Table)
	estimated, err := preflight.EstimateRows(ctx, writer.MysqlClient, table, writer.OriginWhereClause)
	if err != nil {
		log.Logger.Warnf("preflight EXPLAIN failed (continuing): %v", err)
		return nil
	}

	result := preflight.Result{EstimatedRows: estimated, Threshold: threshold}
	log.Logger.Infof("preflight: estimated_rows=%d threshold=%d", result.EstimatedRows, result.Threshold)

	if preflight.NeedsConfirmation(result, config.AutoConfirm) {
		ok, confErr := preflight.ConfirmLargeDelete(os.Stdin, os.Stderr, result)
		if confErr != nil {
			return confErr
		}
		if !ok {
			return fmt.Errorf(
				"large delete confirmation rejected (estimated_rows=%d >= threshold=%d)",
				result.EstimatedRows, result.Threshold,
			)
		}
	}
	return nil
}

func getStopTime(ctx context.Context, sl *lag_checker.SlaveChecker, bucketNum chan int64, rateLimiter *RateLimiter, writer *mysql.Writer) {
	for {
		select {
		case <-ctx.Done():
			log.Logger.Debug("get stop time is finished")
			return
		default:
		}

		if writer.IsFinished() {
			log.Logger.Debug("get stop time is finished")
			return
		}

		var (
			token  int64
			lag    int64
			lagErr error
		)
		if sl != nil {
			log.Logger.Debug("start to get slave check lag")
			lagErr = sl.CheckLag(ctx)
			if lagErr != nil {
				log.Logger.Errorf("slave check lag got err: %v", lagErr)
			} else {
				lag = sl.MaxLag
			}
		}

		if sl == nil || lagErr != nil {
			token = rateLimiter.bucketErrHandle()
		} else {
			rateLimiter.SetCurrentLag(lag)
			log.Logger.Debugf("sl.MaxLag: %d", lag)
			if rateLimiter.ShouldThrottle(lag) {
				log.Logger.Debugf("Reach maxLag Threshold[MaxLag: %d,throttle: %d]", lag, rateLimiter.GetMaxLag())
				rateLimiter.AddCorrect(50)
				if len(bucketNum) < 500 {
					select {
					case <-ctx.Done():
						return
					case bucketNum <- vars.LagThreshold:
					default:
					}
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(800 * time.Millisecond):
				}
				continue
			}

			token = rateLimiter.bucketHandle(lag)
		}

		correct := rateLimiter.GetCorrect()
		log.Logger.Debugf("bucketNum: %d", token+correct)
		log.Logger.Debugf("len of bucketNum: %d", len(bucketNum))
		log.Logger.Debugf("sleep of getStopTime: %s", writer.GetCostTime())

		if len(bucketNum) < 500 {
			select {
			case <-ctx.Done():
				return
			case bucketNum <- token + correct:
			default:
			}
		}
		rateLimiter.DecayCorrect(300)

		sleepTime := writer.GetCostTime() * 5 / 4
		if sleepTime <= 0 {
			sleepTime = 100 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleepTime):
		}
	}
}

// --------PrintProgress----------

// PrintProgress print all running tasks progress every interval
func PrintProgress(config *conf.Config, writer *mysql.Writer, interval time.Duration, ctx context.Context, doneChan chan struct{}) {
	start := time.Now()
	print("\033[2J\033[H") // clear screen and move the cursor to the top-left corner of the screen
	// clear screen
	screenHeight, err := getScreenHeight()
	if err == nil {
		for i := 1; i <= screenHeight; i++ {
			fmt.Printf("\033[%d;1H\033[K", i)
		}
	}

	// fixed part
	print("\033[H")
	print(color.CyanString("[Execute SQL]: "))
	fmt.Printf("[%s]: %s\n", writer.SqlType, writer.ExecuteSQL)
	print(color.CyanString("[Source]: "))
	fmt.Printf("%s:%d\n", config.Host, config.Port)
	print(color.CyanString("[Schema]: "))
	fmt.Printf("%s.%s\n", writer.Database, writer.Table)
	// verbose info
	fmt.Println("=============================================")
	color.Cyan("%-23s%-10s%-10s\n", "Time", "Elapsed", "RowAffects")
	fmt.Printf("%-23s%-10s%-10s\n", "----", "-------", "----------")

	for {
		elapsedTime := time.Since(start)
		e := (int64(elapsedTime.Nanoseconds()) / vars.Billion) * vars.Billion
		select {
		case <-ctx.Done():
			// return when all tasks Done
			rowAffects := writer.GetRowAffects()
			speed := 0.0
			if elapsedTime.Seconds() > 0 {
				speed = float64(rowAffects) / elapsedTime.Seconds()
			}
			color.Green("Total Processed Rows: %d, speed: %.2f rows/s, spend Time: %s\n",
				rowAffects, speed, time.Duration(e).String())
			fmt.Println("exiting...")
			doneChan <- struct{}{}
			return
		default:
			rowAffects := writer.GetRowAffects()
			fmt.Printf("%-23s", time.Now().Format("2006-01-02 15:04:05"))
			fmt.Printf("%-10s", time.Duration(e).String())
			fmt.Printf("%-10d\n", rowAffects)
			time.Sleep(interval)
		}
	}
}

func getScreenHeight() (height int, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("Recovered from panic in getScreenHeight: %v", r)
			err = errors.New("panic in getScreenHeight")
		}
	}()

	// set a default value for height
	height = 40
	screen, err := tcell.NewScreen()
	if err != nil {
		return
	}
	return screenHeight(screen)
}

func screenHeight(screen tcell.Screen) (height int, err error) {
	height = 40
	if err = screen.Init(); err != nil {
		return
	}
	// tcell's shutdown channel is only created after successful initialization.
	defer screen.Fini()
	_, height = screen.Size()
	return
}

func runProgressCallback(
	ctx context.Context,
	rateLimiter *RateLimiter,
	writer *mysql.Writer,
	interval time.Duration,
	callback ProgressCallback,
	doneChan chan struct{},
) {
	defer func() {
		if r := recover(); r != nil {
			log.Logger.Errorf("recovered panic in runProgressCallback: %v", r)
		}
		select {
		case doneChan <- struct{}{}:
		default:
		}
	}()

	start := time.Now()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	callbackToken := make(chan struct{}, 1)
	callbackToken <- struct{}{}

	asyncInvoke := func(snapshot *ProgressSnapshot) {
		select {
		case <-callbackToken:
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Logger.Errorf("recovered panic in progress callback: %v", r)
					}
					callbackToken <- struct{}{}
				}()
				callback(snapshot)
			}()
		default:
			log.Logger.Warn("progress callback is still running, skip current tick")
		}
	}

	for {
		select {
		case <-ctx.Done():
			asyncInvoke(&ProgressSnapshot{
				RowAffects:   writer.GetRowAffects(),
				ElapsedTime:  time.Since(start),
				CurrentSleep: rateLimiter.GetSleep(),
				MaxSlaveLag:  rateLimiter.GetCurrentLag(),
				IsFinished:   writer.IsFinished(),
			})
			return
		case <-ticker.C:
			asyncInvoke(&ProgressSnapshot{
				RowAffects:   writer.GetRowAffects(),
				ElapsedTime:  time.Since(start),
				CurrentSleep: rateLimiter.GetSleep(),
				MaxSlaveLag:  rateLimiter.GetCurrentLag(),
				IsFinished:   writer.IsFinished(),
			})
		}
	}
}

func normalizeStopError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return ErrExecutionStopped
	}
	return err
}
