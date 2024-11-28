package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/gdamore/tcell"
	"github.com/juju/ratelimit"

	"go-oak-chunk/v2/conf"
	"go-oak-chunk/v2/log"
	"go-oak-chunk/v2/mysql"
	"go-oak-chunk/v2/task/lag_checker"
	"go-oak-chunk/v2/vars"
)

func RunTask(config *conf.Config) error {
	// print json of config
	configJson, err := json.Marshal(&config)
	if err == nil {
		log.StreamLogger.Debug("config json: %s", string(configJson))
	}

	var wg sync.WaitGroup
	bucketNum := make(chan int64, 1000)
	bucket := ratelimit.NewBucketWithQuantum(1*time.Millisecond, 1, 1)

	// 1. 创建执行SQL的协程
	// 包含预检查
	w := mysql.NewWriter(config)

	// 2. 检查是否要创建检查slaveLag的协程
	// 3. 检查是否要创建检查mysqlio延迟的协程
	sl, err := lag_checker.NewSlaveChecker(w.MysqlClient, config)
	if err != nil {
		log.StreamLogger.Error("create SlaveChecker goroutine is failed, err: %v", err)
	}

	wg.Add(1)
	go func() {
		getStopTime(sl, bucketNum, config, w)
		log.StreamLogger.Debug("getStopTime goroutine is finished")
		wg.Done()
	}()

	// 4. read
	readErrChan := make(chan error)
	p := mysql.NewProcedure(w)
	wg.Add(1)
	go func() {
		// equals to read goroutine
		readErrChan <- p.BuildSQL(w.ProducerQueue, &wg)
	}()

	// 5. write
	writeErrChan := make(chan error)
	wg.Add(1)
	go func() {
		// write goroutine
		writeErrChan <- w.Write(bucket, bucketNum, &wg)
	}()

	tasksDoneChan := make(chan struct{})
	go func() {
		wg.Wait()
		tasksDoneChan <- struct{}{}
	}()

	// 6. if verbose
	ctx, cancel := context.WithCancel(context.Background())
	printProgressDoneChan := make(chan struct{})
	if config.PrintProgress {
		go PrintProgress(config, w, 3*time.Second, ctx, printProgressDoneChan)
	}

	for {
		select {
		case readErr := <-readErrChan:
			if readErr != nil {
				Close(sl, w, bucketNum)
				cancel()
				return readErr
			} else {
				continue
			}
		case writeErr := <-writeErrChan:
			if writeErr != nil {
				Close(sl, w, bucketNum)
				cancel()
				return writeErr
			} else {
				continue
			}
		case <-tasksDoneChan:
			Close(sl, w, bucketNum)
			// tell PrintProgress to stop
			cancel()
			// confirm PrintProgress stopped
			if config.PrintProgress {
				<-printProgressDoneChan
			}
			return nil
		}
	}
}

func getStopTime(sl *lag_checker.SlaveChecker, bucketNum chan int64, c *conf.Config, w *mysql.Writer) {
	var (
		slaveWg  sync.WaitGroup
		errSalve error
	)

	slaveWg.Add(1)
	go func() {
		for !w.IsFinished && sl != nil {
			log.StreamLogger.Debug("start to get slave check lag")
			errSalve = sl.CheckLag()
			if errSalve != nil {
				log.StreamLogger.Error("slave check lag got err: %v", errSalve)
				break
			}
			time.Sleep(800 * time.Millisecond)
		}
		log.StreamLogger.Debug("get slave check lag is finished")
		slaveWg.Done()
	}()

	for !w.IsFinished {
		var token int64
		if errSalve != nil || sl == nil {
			token = bucketErrHandle(c)
		} else {
			log.StreamLogger.Debug("sl.MaxLag: %d", sl.MaxLag)
			if sl.MaxLag >= c.MaxLag && c.MaxLag > 0 {
				log.StreamLogger.Debug("Reach maxLag Threshold[MaxLag: %d,throttle: %d]", sl.MaxLag, c.MaxLag)
				c.Correct += 50

				// 增加一个防止chan的容量达到上限的机制 at 2024-03-07
				if len(bucketNum) < 500 {
					bucketNum <- vars.LagThreshold
				}
				time.Sleep(800 * time.Millisecond)
				continue
			}

			token = bucketHandle(sl.MaxLag, c)
		}
		log.StreamLogger.Debug("bucketNum: %d", token+c.Correct)
		log.StreamLogger.Debug("len of bucketNum: %d", len(bucketNum))
		log.StreamLogger.Debug("sleep of getStopTime: %d", w.CostTime)
		// 增加一个防止chan的容量达到上限的机制 at 2024-03-07
		if len(bucketNum) < 500 {
			bucketNum <- token + c.Correct
		}
		// magic number
		if c.Correct > 300 {
			c.Correct--
		}
		time.Sleep(w.CostTime / 4 * 5)
	}
	log.StreamLogger.Debug("get stop time is finished")
	slaveWg.Wait()
}

// bucketHandle slaveLag是个非负整数、单位为秒，我们考虑sleep时间的浮动的时候，应该考虑如下几种情况
// 一、 NotConsiderLag不生效时
//  1. slaveLag <= c.Sleep 此时，我们应该将bucket tokens和slaveLag进行绑定，说不定不用直接顶满c.Sleep就可以消除主从延迟
//  2. slaveLag > c.Sleep && slaveLag-c.Sleep > 60*n 则实际等待时间要slaveLag+n
//
// 二、 NotConsiderLag生效时
//
//	实际等待时间最大为c.Sleep
//
// 为了避免逻辑混乱，
//
//	使用者在用了sleep参数后，会进行(c.sleep-1, c.sleep]的sleep时间
//	如果sleep参数的值小于1s，会进行[0, c.sleep)的sleep时间
func bucketHandle(lag int64, c *conf.Config) int64 {
	x := c.Sleep / 1000
	if lag == 0 && c.Sleep > 0 {
		if c.Sleep <= 1000 {
			return rand.Int63n(c.Sleep)
		}
		return rand.Int63n(c.Sleep-(c.Sleep-1000)) + (c.Sleep - 1000)
	} else if lag == 0 && c.Sleep == 0 {
		return 0
	} else {
		if c.NoConsiderLag {
			if lag <= x {
				return lag * 1000
			} else {
				return c.Sleep
			}
		} else {
			if lag <= x || lag+60 <= x {
				return lag * 1000
			} else { // slaveLag > c.Sleep && slaveLag-c.Sleep > 60*n
				plus := (lag - x) / 60
				return (x + plus) * 1000
			}
		}
	}
}

// bucketErrHandle 如果slave检测错误了，就取(c.sleep-1, c.sleep]中的一个随机数
// 如果卡死一个时间可能会很慢
func bucketErrHandle(c *conf.Config) int64 {
	var token int64
	if c.Sleep != 0 {
		x := c.Sleep * 1000
		token = rand.Int63n(x-(x-1000)) + (x - 1000)
	} else {
		token = 0
	}
	return token
}

func Close(sl *lag_checker.SlaveChecker, w *mysql.Writer, bucketNum chan int64) {
	if sl != nil {
		for _, slave := range sl.Slaves {
			_ = slave.MysqlClient.Close()
		}
	}
	w.MysqlClient.Close()
	close(w.ProducerQueue)
	close(bucketNum)
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
		elapsedTime := time.Now().Sub(start)
		e := (int64(elapsedTime.Nanoseconds()) / vars.Billion) * vars.Billion
		select {
		case <-ctx.Done():
			// return when all tasks Done
			color.Green("Total Processed Rows: %d, speed: %.2f rows/s, spend Time: %s\n",
				writer.RowAffects, float64(writer.RowAffects)/elapsedTime.Seconds(), time.Duration(e).String())
			fmt.Println("exiting...")
			doneChan <- struct{}{}
			return
		default:
			fmt.Printf("%-23s", time.Now().Format("2006-01-02 15:04:05"))
			fmt.Printf("%-10s", time.Duration(e).String())
			fmt.Printf("%-10d\n", writer.RowAffects)
			time.Sleep(interval)
		}
	}
}

func getScreenHeight() (height int, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.GlobalLogger.Error("Recovered from panic in getScreenHeight: %v", r)
			err = errors.New("panic in getScreenHeight")
		}
	}()

	// set a default value for height
	height = 40
	screen, err := tcell.NewScreen()
	if err != nil {
		return
	}
	defer screen.Fini()
	if err = screen.Init(); err != nil {
		return
	}
	_, height = screen.Size()
	return
}
