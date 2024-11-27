package mysql

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"go-oak-chunk/v2/conf"
)

func TestBuildSQL(t *testing.T) {
	configPath := "../conf/example.toml"
	config, err := conf.NewConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	config.PreCheck()

	writer := NewWriter(config)
	p := NewProcedure(writer)
	var wg sync.WaitGroup
	errChan := make(chan error)

	wg.Add(1)
	go func() {
		errChan <- p.BuildSQL(writer.ProducerQueue, &wg)
	}()

	// wait task done: create doneChan to wait doneWg done
	doneChan := make(chan struct{})
	go func() {
		for pr := range writer.ProducerQueue {
			fmt.Printf("isFinished: %v\n", pr.IsFinished)
			if pr.IsFinished {
				break
			}

			println(pr.WhereClause)
			for _, value := range pr.CurrentKeyValues {
				fmt.Println(reflect.TypeOf(value.ColumnValue))
				fmt.Printf("%s : %v\n", value.ColumnName, value.ColumnValue)
			}
			fmt.Println("---------------")
		}
		wg.Wait()
		doneChan <- struct{}{}
	}()

	for {
		select {
		case err = <-errChan:
			if err != nil {
				t.Errorf("got error %s", err)
				return
			} else {
				continue
			}
		case <-doneChan:
			return
		}
	}

}
