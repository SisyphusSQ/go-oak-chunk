package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"go-oak-chunk/v2/log"
	"go-oak-chunk/v2/vars"
)

type Procedure struct {
	MysqlClient       *sql.DB
	ChunkSize         int64
	originWhereClause string
	database          string
	table             string
	unqKeys           *UnqKeys
}

type KeyValue struct {
	ColumnName  string
	ColumnValue any
}

func NewProcedure(w *Writer) *Procedure {
	return &Procedure{
		MysqlClient:       w.MysqlClient,
		ChunkSize:         w.ChunkSize,
		originWhereClause: w.OriginWhereClause,
		database:          w.Database,
		table:             w.Table,
		unqKeys:           w.unqKeys,
	}
}

func (p *Procedure) BuildSQL(producer chan *Producer, wg *sync.WaitGroup) error {
	if p.ChunkSize == 0 {
		pr := &Producer{
			WhereClause:      "",
			IsFinished:       true,
			CurrentKeyValues: make([]*KeyValue, 0),
		}
		producer <- pr
		wg.Done()
		return nil
	}

	// build select stmt
	keyList := getKeyList(p.unqKeys)
	keyColumns := strings.Join(keyList, ",")
	conditions := BuildSelectWhereClause(p.unqKeys)

	firstSql := fmt.Sprintf(vars.FirstSQL, keyColumns, p.database+"."+p.table, p.originWhereClause)
	nextSql := firstSql
	/*
		if p.ChunkSize > 1 {
			nextSql += fmt.Sprintf(" AND %s ", conditions[">"])
		} else {
			nextSql += fmt.Sprintf(" AND %s ", conditions[">="])
		}
	*/
	nextSql += fmt.Sprintf(" AND %s ", conditions[">"])
	firstSql += fmt.Sprintf(" ORDER BY %s LIMIT %d ", keyColumns, p.ChunkSize)
	if p.ChunkSize > 1 {
		nextSql += fmt.Sprintf(" ORDER BY %s LIMIT %d ", keyColumns, p.ChunkSize)
	} else {
		nextSql += fmt.Sprintf(" ORDER BY %s LIMIT %d ", keyColumns, 1000)
	}

	// build execute stmt
	var execWhere string
	if p.ChunkSize == 1 {
		// index can't be not unique
		execWhere = BuildBulkExecWhereClause(p.unqKeys)
	} else {
		execWhere = fmt.Sprintf(" AND (%s AND %s) limit %d", conditions[">="], conditions["<="], p.ChunkSize)
	}

	log.StreamLogger.Debug("firstSql: [%s]", firstSql)
	log.StreamLogger.Debug("nextSql: [%s]", nextSql)
	// prepare is finished
	// --------------------------------------
	// start select index value(s)
	// note: chunkSize == 1 or chunkSize > 1
	fetchSql := firstSql
	selectKeyCols := make([]*KeyValue, 0, len(p.unqKeys.UniqueKeyColumns))
	for {
		if fetchSql == firstSql {
			if p.ChunkSize > 1 {
				keyValues, isFinished, err := p.fetchFistAndLastData(fetchSql)
				if err != nil {
					log.StreamLogger.Error("BuildSQL got err: %v", err)
					return err
				}

				pr := &Producer{
					WhereClause:      execWhere,
					IsFinished:       isFinished,
					CurrentKeyValues: keyValues,
				}
				producer <- pr
				if isFinished {
					wg.Done()
					return nil
				}

				fetchSql = nextSql
				selectKeyCols = keyValues[len(keyValues)-len(p.unqKeys.UniqueKeyColumns):]
				continue
			} else { // if p.ChunkSize == 1
				rows, err := p.MysqlClient.Query(fetchSql)
				if err != nil {
					log.StreamLogger.Error("BuildSQL got err: %v", err)
					return err
				}

				cols, err := rows.Columns()
				if err != nil {
					log.StreamLogger.Error("BuildSQL got err: %v", err)
					return err
				}

				keyValues := make([]*KeyValue, 0, len(p.unqKeys.UniqueKeyColumns))
				for rows.Next() {
					keyValues, err = p.getSingleData(cols, rows)
					if err != nil {
						log.StreamLogger.Error("BuildSQL got err: %v", err)
						return err
					}

					pr := &Producer{
						WhereClause:      execWhere,
						IsFinished:       false,
						CurrentKeyValues: keyValues,
					}
					producer <- pr
				}

				if len(keyValues) == 0 {
					log.StreamLogger.Debug("fetch index data is finished")
					pr := &Producer{
						WhereClause:      execWhere,
						IsFinished:       true,
						CurrentKeyValues: keyValues,
					}
					producer <- pr
					wg.Done()
					return nil
				}

				fetchSql = nextSql
				selectKeyCols = keyValues
				rows.Close()
				continue
			}
		}

		args := getArgs(selectKeyCols)
		//log.StreamLogger.Debug("Args values: %v", args)
		if p.ChunkSize > 1 {
			keyValues, isFinished, err := p.fetchFistAndLastData(fetchSql, args...)
			if err != nil {
				log.StreamLogger.Error("BuildSQL got err: %v", err)
				return err
			}

			pr := &Producer{
				WhereClause:      execWhere,
				IsFinished:       isFinished,
				CurrentKeyValues: keyValues,
			}
			producer <- pr
			if isFinished {
				wg.Done()
				return nil
			}

			selectKeyCols = keyValues[len(keyValues)-len(p.unqKeys.UniqueKeyColumns):]
		} else { // if p.ChunkSize == 1
			//log.StreamLogger.Debug("fetchSql: %s", fetchSql)
			rows, err := p.MysqlClient.Query(fetchSql, args...)
			if err != nil {
				log.StreamLogger.Error("BuildSQL got err: %v", err)
				return err
			}

			cols, err := rows.Columns()
			if err != nil {
				log.StreamLogger.Error("BuildSQL got err: %v", err)
				return err
			}

			keyValues := make([]*KeyValue, 0, len(p.unqKeys.UniqueKeyColumns))
			for rows.Next() {
				keyValues, err = p.getSingleData(cols, rows)
				if err != nil {
					log.StreamLogger.Error("BuildSQL got err: %v", err)
					return err
				}

				pr := &Producer{
					WhereClause:      execWhere,
					IsFinished:       false,
					CurrentKeyValues: keyValues,
				}
				producer <- pr
			}
			rows.Close()

			if len(keyValues) == 0 {
				log.StreamLogger.Debug("fetch index data is finished")
				pr := &Producer{
					WhereClause:      execWhere,
					IsFinished:       true,
					CurrentKeyValues: keyValues,
				}
				producer <- pr
				wg.Done()
				return nil
			}

			selectKeyCols = keyValues
		}
	}
}

func (p *Procedure) fetchFistAndLastData(fetchSql string, args ...any) ([]*KeyValue, bool, error) {
	resKeyValues := make([]*KeyValue, 0)
	lastKeyValues := make([]*KeyValue, 0)
	//log.StreamLogger.Debug("fetchSql: %s", fetchSql)
	rows, err := p.MysqlClient.Query(fetchSql, args...)
	defer rows.Close()
	if err != nil {
		log.StreamLogger.Error("fetchFistAndLastData got err: %v", err)
		return nil, false, err
	}

	cols, err := rows.Columns()
	if err != nil {
		log.StreamLogger.Error("fetchFistAndLastData got err: %v", err)
		return nil, false, err
	}

	for rows.Next() {
		// todo: how to handle null value
		scanArgs := make([]interface{}, len(cols))
		for i := range scanArgs {
			scanArgs[i] = &sql.RawBytes{}
		}

		if err = rows.Scan(scanArgs...); err != nil {
			log.StreamLogger.Error("fetchFistAndLastData Scan got err: %v", err)
			return nil, false, err
		}

		// first row
		if len(resKeyValues) == 0 {
			for i, keyCol := range p.unqKeys.UniqueKeyColumns {
				value, err := handleColumnValue(scanArgs, cols, keyCol, p.unqKeys.UniqueKeyTypes[i])
				if err != nil {
					return nil, false, err
				}
				keyValue := &KeyValue{
					ColumnName:  keyCol,
					ColumnValue: value,
				}
				resKeyValues = append(resKeyValues, keyValue)
			}
			continue
		}

		tmpKeyValues := make([]*KeyValue, 0)
		for i, keyCol := range p.unqKeys.UniqueKeyColumns {
			value, err := handleColumnValue(scanArgs, cols, keyCol, p.unqKeys.UniqueKeyTypes[i])
			if err != nil {
				return nil, false, err
			}
			keyValue := &KeyValue{
				ColumnName:  keyCol,
				ColumnValue: value,
			}
			tmpKeyValues = append(tmpKeyValues, keyValue)
		}
		lastKeyValues = tmpKeyValues
	}

	// 处理在某些情况下倒数第二次只能取到first index value的情况
	if len(lastKeyValues) == 0 {
		lastKeyValues = resKeyValues
	}

	// 最后一次的情况
	if len(resKeyValues) == 0 {
		// empty set
		return nil, true, nil
	}
	return append(resKeyValues, lastKeyValues...), false, nil
}

func (p *Procedure) getSingleData(cols []string, rows *sql.Rows) ([]*KeyValue, error) {
	keyValues := make([]*KeyValue, 0, len(cols))
	scanArgs := make([]interface{}, len(cols))
	for i := range scanArgs {
		scanArgs[i] = &sql.RawBytes{}
	}

	if err := rows.Scan(scanArgs...); err != nil {
		log.StreamLogger.Error("getSingleData Scan got err: %v", err)
		return nil, err
	}

	for i, keyCol := range p.unqKeys.UniqueKeyColumns {
		value, err := handleColumnValue(scanArgs, cols, keyCol, p.unqKeys.UniqueKeyTypes[i])
		if err != nil {
			return nil, err
		}
		keyValue := &KeyValue{
			ColumnName:  keyCol,
			ColumnValue: value,
		}
		keyValues = append(keyValues, keyValue)
	}
	return keyValues, nil
}

func getColumnValueOld(keyValues []*KeyValue) []any {
	values := make([]any, 0, len(keyValues))
	for _, keyCol := range keyValues {
		values = append(values, keyCol.ColumnValue)
	}
	return values
}

func getColumnValue(keyValues []*KeyValue, chunkSize int64) []any {
	values := make([]any, 0, len(keyValues)*2)
	if len(keyValues) == 2 || chunkSize == 1 {
		for _, keyCol := range keyValues {
			values = append(values, keyCol.ColumnValue)
		}
	} else { // len > 2 || chunkSize > 1
		// Sample:
		// (((`id` > ?) OR (`id` = ? AND `c` > ?) OR (`id` = ? AND `c` = ? AND `created_at` >= ?))) AND
		// (((`id` < ?) OR (`id` = ? AND `c` < ?) OR (`id` = ? AND `c` = ? AND `created_at` <= ?)))

		// len(keyValues) 必定是偶数
		// 前半
		for i := 0; i < len(keyValues)/2; i++ {
			for j := 0; j <= i; j++ {
				values = append(values, keyValues[j].ColumnValue)
			}
		}

		// 后半
		for i := len(keyValues) / 2; i < len(keyValues); i++ {
			for j := len(keyValues) / 2; j <= i; j++ {
				values = append(values, keyValues[j].ColumnValue)
			}
		}
	}

	return values
}

func getArgs(keyValues []*KeyValue) []any {
	values := make([]any, 0)
	for i := 0; i < len(keyValues); i++ {
		for j := 0; j <= i; j++ {
			values = append(values, keyValues[j].ColumnValue)
		}
	}

	return values
}

func getKeyList(unqKeys *UnqKeys) []string {
	keys := make([]string, 0, len(unqKeys.UniqueKeyColumns))
	for _, column := range unqKeys.UniqueKeyColumns {
		keys = append(keys, "`"+column+"`")
	}
	return keys
}
