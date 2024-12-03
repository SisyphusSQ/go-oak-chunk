package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	soar "github.com/XiaoMi/soar/ast"
	"github.com/juju/ratelimit"
	"github.com/pingcap/parser/ast"

	"go-oak-chunk/v2/conf"
	"go-oak-chunk/v2/log"
	"go-oak-chunk/v2/vars"
)

type Writer struct {
	MysqlClient       *sql.DB
	ExecuteSQL        string
	OriginWhereClause string
	ChunkSize         int64
	TxnSize           int64
	IsFinished        bool
	SqlType           string
	RowAffects        int64
	CostTime          time.Duration
	Database          string
	Table             string
	noLogBing         bool
	unqKeys           *UnqKeys
	ProducerQueue     chan *Producer
}

type UnqKeys struct {
	UniqueKeyColumns []string
	CountColumns     int
	UniqueKeyTypes   []byte
	IsNull           []bool
	Tp               int
}

type Producer struct {
	WhereClause      string
	IsFinished       bool
	CurrentKeyValues []*KeyValue
}

type Proceed struct {
	WhereClause string
	RangeStarts []int64
	RangeEnds   []int64
	IsFinished  bool
}

func NewWriter(c *conf.Config) *Writer {
	w := &Writer{
		noLogBing:     c.NoLogBin,
		ChunkSize:     c.ChunkSize,
		TxnSize:       c.TxnSize,
		ExecuteSQL:    strings.ReplaceAll(c.ExecuteQuery, ";", ""),
		ProducerQueue: make(chan *Producer, 1000),
		IsFinished:    false,
		CostTime:      1 * time.Second,
	}
	w.preCheck(c)
	return w
}

func (w *Writer) preCheck(c *conf.Config) {
	var err error

	// 获取database和table
	//w.Table = c.Table
	w.Database = c.Database
	if w.Database == "" {
		log.StreamLogger.Error("No Database/Table specified. Specify Table with -t or --Table and Database with -d or --Database")
		os.Exit(1)
	}

	w.Table, err = TableMetaInfo(w.ExecuteSQL)
	if err != nil {
		log.StreamLogger.Error("Table failed. %s", err.Error())
		os.Exit(1)
	}

	// init mysql connect
	w.MysqlClient, err = NewMysqlClient(c)
	if err != nil {
		log.StreamLogger.Error("open connect is failed, err: %+v", err)
		os.Exit(1)
	}

	if !w.tableExists() {
		log.StreamLogger.Error("Table %s.%s does not exist", w.Database, w.Table)
		os.Exit(1)
	}

	err = w.getInfoFromTable(c)
	if err != nil {
		log.StreamLogger.Error("sql parser is failed,please check whether sql is correct, err: %+v", err)
		os.Exit(1)
	}
}

func (w *Writer) Write(bucket *ratelimit.Bucket, bucketNum chan int64, wg *sync.WaitGroup) error {
	maxRetry := 3

	for {
		// get last bucket number
		var bucketCount int64
		for i := 0; i < len(bucketNum); i++ {
			bucketCount = <-bucketNum
		}
		if bucketCount == vars.LagThreshold {
			log.StreamLogger.Debug("Sleep 1s to let slave eliminate lag")
			bucket.Wait(1000)
			continue
		}

		log.StreamLogger.Debug("bucketCount: %d", bucketCount)
		bucket.Wait(bucketCount)

		var rowAffects int64
		beginTime := time.Now()
		tx, err := w.MysqlClient.Begin()
		if err != nil {
			return err
		}

		for pr := range w.ProducerQueue {
			if pr.IsFinished {
				log.StreamLogger.Debug("Get whereClause is finished")
				w.IsFinished = true
				break
			}

			// 在这里组装完sql和参数后，传到writer中去
			execSql := w.ExecuteSQL + pr.WhereClause
			values := getColumnValue(pr.CurrentKeyValues, w.ChunkSize)

			log.StreamLogger.Debug("execSql: %s", execSql)
			log.StreamLogger.Debug("parma values: %v", values)

			res, errEx := tx.Exec(execSql, values...)
			if errEx != nil {
				// 重试机制 todo 重写一个方法
				var errEx2 error
				for i := 0; i < maxRetry; i++ {
					tx, errEx2 = w.MysqlClient.Begin()
					if errEx2 != nil {
						return errEx2
					}

					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					res, errEx2 = tx.ExecContext(ctx, execSql, values...)
					cancel()
					if errEx2 != nil {
						continue
					} else {
						break
					}
				}

				if errEx2 != nil {
					return errEx
				}
			}

			// 算一下chunk-size和txn-size之间的关系
			affects, _ := res.RowsAffected()
			rowAffects += affects
			if rowAffects >= w.TxnSize {
				break
			}
		}

		// 速度的控制应该在txnSize
		// pt-archiver是在事务结束(commit)之后，才进行sleep
		err = tx.Commit()
		if err != nil {
			return err
		}
		w.RowAffects += rowAffects
		w.CostTime = time.Now().Sub(beginTime)

		// finish flag
		if w.IsFinished {
			log.StreamLogger.Debug("Execute SQL is finished successfully")
			wg.Done()
			return nil
		}
	}
}

func (w *Writer) tableExists() bool {
	var count int
	err := w.MysqlClient.QueryRow(vars.TableExistsSQL, w.Database, w.Table).Scan(&count)
	if err != nil {
		log.StreamLogger.Error("tableExists scan failed, err:%v", err)
		os.Exit(1)
	}
	return count == 1
}

// getInfoFromTable use tidb parser to build necessary info
// fetch sql type
// fetch index
func (w *Writer) getInfoFromTable(c *conf.Config) error {
	// First get sql type, update or delete?
	sqlStmt, err := soar.TiParse(w.ExecuteSQL, "", "")
	if err != nil {
		return err
	}

	if len(sqlStmt) == 0 || len(sqlStmt) > 1 {
		log.StreamLogger.Error("SQL is empty? or SQL number is over 1? pls confirm SQL number is only 1")
		os.Exit(1)
	}

	node := sqlStmt[0]
	v := &visitor{}
	switch node.(type) {
	case *ast.DeleteStmt:
		w.SqlType = "Delete"
		node.Accept(v)

		w.ExecuteSQL = fmt.Sprintf("DELETE FROM `%s` WHERE ", w.Table)
	case *ast.UpdateStmt:
		w.SqlType = "Update"
		node.Accept(v)

		re := regexp.MustCompile(`set.*where|SET.*WHERE|set.*WHERE|SET.*where`)
		sub := re.FindString(c.ExecuteQuery)
		w.ExecuteSQL = fmt.Sprintf("UPDATE `%s` %s ", w.Table, sub)
	default:
		log.StreamLogger.Error("please confirm sql type is `update` or `delete`")
		os.Exit(1)
	}

	if v.whereClause != "" {
		// avoid where clause "or", make program confused
		w.OriginWhereClause = fmt.Sprintf("(%s)", v.whereClause)
		log.StreamLogger.Debug("originWhereClause: [%s]", v.whereClause)

		w.ExecuteSQL += w.OriginWhereClause
	}

	// Second find primary/unique index which can be used
	// check for column in Table meta
	var tableMeta string
	rows, err := w.MysqlClient.Query(fmt.Sprintf(vars.TableInfoSQL, w.Database+"."+w.Table))
	if err != nil {
		log.StreamLogger.Error("`show create Table %s` got err: %v", w.Database+"."+w.Table, err)
		os.Exit(1)
	}

	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.StreamLogger.Error("`show create Table %s` got err: %v", w.Database+"."+w.Table, err)
		os.Exit(1)
	}

	for rows.Next() {
		scanArgs := make([]interface{}, len(cols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}

		if err = rows.Scan(scanArgs...); err != nil {
			log.StreamLogger.Error("`show create Table %s` got err: %v", w.Database+"."+w.Table, err)
			os.Exit(1)
		}
		tableMeta = ColumnValue(scanArgs, cols, "Create Table")
	}

	tableStmt, err := soar.TiParse(tableMeta, "", "")
	if err != nil {
		return err
	}

	uks := make([]*UnqKeys, 0)
	tableNode := tableStmt[0]
	switch tableNode.(type) {
	case *ast.CreateTableStmt:
		uks = GetPossibleUniqueKeys(tableNode.(*ast.CreateTableStmt))
		if len(uks) == 0 {
			log.StreamLogger.Error("Can't find any index which is primary or unique key")
			os.Exit(1)
		}
	default:
		log.StreamLogger.Error("tableMeta is not CreateTableStmt, something goes wrong, tableMeta: %s", tableMeta)
		os.Exit(1)
	}

	if c.ForceChunkingColumn != "" {
		uniqueColumns := strings.Split(c.ForceChunkingColumn, ",")
		sort.Strings(uniqueColumns)
		for _, uk := range uks {
			sortKeys := make([]string, len(uk.UniqueKeyColumns))
			copy(sortKeys, uk.UniqueKeyColumns)
			sort.Strings(sortKeys)
			if reflect.DeepEqual(uniqueColumns, sortKeys) {
				w.unqKeys = uk
				return nil
			}
		}

		// 如果for结束没有数据，说明使用者瞎写的ForceChunkingColumn
		log.StreamLogger.Error("forced_chunking_column doesn't conform to primary or unique key, ForceChunkingColumn: %s", c.ForceChunkingColumn)
		os.Exit(1)
	}

	for _, uk := range uks {
		if uk.Tp == vars.ConstraintPrimaryKey {
			w.unqKeys = uk
			return nil
		}
	}

	w.unqKeys = uks[0]
	return nil
}

func (w *Writer) lockTableRead() {
	_, err := w.MysqlClient.Exec(fmt.Sprintf(vars.LockTableSQL, w.Database, w.Table))
	if err != nil {
		log.StreamLogger.Error("lockTableRead failed, err:%v", err)
		os.Exit(1)
	}
}

func (w *Writer) unlockTable() {
	_, err := w.MysqlClient.Exec(vars.UnlockTableSQL)
	if err != nil {
		log.StreamLogger.Error("lockTableRead failed, err:%v", err)
		os.Exit(1)
	}
}
