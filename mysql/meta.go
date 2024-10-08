package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	soar "github.com/XiaoMi/soar/ast"
	"github.com/XiaoMi/soar/common"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	"github.com/tidwall/gjson"
)

type SlaveHost struct {
	ServerId  int64
	Host      string
	Port      int
	MasterId  string
	SlaveUUID string
}

type visitor struct {
	whereClause string
}

func (v *visitor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

func (v *visitor) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.UpdateStmt:
		buf := new(strings.Builder)
		if x.Where != nil {
			x.Where.Restore(&format.RestoreCtx{
				Flags: format.DefaultRestoreFlags,
				In:    buf,
			})
		}
		s := strings.ReplaceAll(buf.String(), "_UTF8MB4", "")
		s = strings.ReplaceAll(s, "_UTF8", "")
		v.whereClause = s
	case *ast.DeleteStmt:
		buf := new(strings.Builder)
		if x.Where != nil {
			x.Where.Restore(&format.RestoreCtx{
				Flags: format.DefaultRestoreFlags,
				In:    buf,
			})
		}
		s := strings.ReplaceAll(buf.String(), "_UTF8MB4", "")
		s = strings.ReplaceAll(s, "_UTF8", "")
		v.whereClause = s
	}
	return in, true
}

func columnIndex(slaveCols []string, colName string) int {
	for idx := range slaveCols {
		if slaveCols[idx] == colName {
			return idx
		}
	}
	return -1
}

func ColumnValueAny(scanArgs []interface{}, slaveCols []string, colName string) any {
	var c = columnIndex(slaveCols, colName)
	if c == -1 {
		return nil
	}
	return scanArgs[c]
}

func ColumnValue(scanArgs []interface{}, slaveCols []string, colName string) string {
	var c = columnIndex(slaveCols, colName)
	if c == -1 {
		return ""
	}
	return string(*scanArgs[c].(*sql.RawBytes))
}

func ColumnValueInt64(scanArgs []interface{}, slaveCols []string, colName string) (int64, error) {
	var c = columnIndex(slaveCols, colName)
	if c == -1 {
		return 0, nil
	}

	v := string(*scanArgs[c].(*sql.RawBytes))
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, err
	}

	return i, nil
}

func ColumnValueUInt64(scanArgs []interface{}, slaveCols []string, colName string) (uint64, error) {
	var c = columnIndex(slaveCols, colName)
	if c == -1 {
		return 0, nil
	}

	v := string(*scanArgs[c].(*sql.RawBytes))
	i, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, err
	}

	return i, nil
}

func GetPossibleUniqueKeys(tableNode *ast.CreateTableStmt) []*UnqKeys {
	unqKeys := make([]*UnqKeys, 0)
	for _, constraint := range tableNode.Constraints {
		// get primary or unique keys
		if constraint.Tp != ast.ConstraintPrimaryKey && constraint.Tp != ast.ConstraintUniq &&
			constraint.Tp != ast.ConstraintUniqKey && constraint.Tp != ast.ConstraintUniqIndex {
			continue
		}

		unqKey := &UnqKeys{
			UniqueKeyColumns: make([]string, 0),
			CountColumns:     0,
			UniqueKeyTypes:   make([]byte, 0),
			IsNull:           make([]bool, 0),
			Tp:               int(constraint.Tp),
		}
		for _, key := range constraint.Keys {
			unqKey.UniqueKeyColumns = append(unqKey.UniqueKeyColumns, key.Column.Name.String())

			for _, col := range tableNode.Cols {
				isNull := true

				if key.Column.Name.String() != col.Name.Name.String() {
					//log.StreamLogger.Debug("key: %s <-> col: %s", key.Column.Name.String(), col.Name.String())
					continue
				}
				for _, option := range col.Options {
					if option.Tp == ast.ColumnOptionNotNull {
						isNull = false
						break
					}
				}

				unqKey.UniqueKeyTypes = append(unqKey.UniqueKeyTypes, col.Tp.Tp)
				unqKey.IsNull = append(unqKey.IsNull, isNull)
			}
		}
		unqKey.CountColumns = len(unqKey.UniqueKeyColumns)
		unqKeys = append(unqKeys, unqKey)
	}

	return unqKeys
}

func handleColumnValue(scanArgs []interface{}, cols []string, keyCol string, tp byte) (any, error) {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24, mysql.TypeLonglong:
		value, err := ColumnValueInt64(scanArgs, cols, keyCol)
		if err != nil {
			if tp != mysql.TypeLonglong {
				return nil, err
			}

			// 没有获取unsigned值，所以先用迂回的方法处理
			unsignedBigInt, errParse := ColumnValueUInt64(scanArgs, cols, keyCol)
			if errParse != nil {
				return nil, errParse
			}
			return unsignedBigInt, errParse
		}
		return value, err
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate:
		return ColumnValue(scanArgs, cols, keyCol), nil
	case mysql.TypeFloat:
		strValue := ColumnValue(scanArgs, cols, keyCol)
		value, err := strconv.ParseFloat(strValue, 32)
		if err != nil {
			return nil, err
		}
		return float32(value), err
	case mysql.TypeDouble:
		strValue := ColumnValue(scanArgs, cols, keyCol)
		value, err := strconv.ParseFloat(strValue, 64)
		if err != nil {
			return nil, err
		}
		return value, err
	default:
		return nil, errors.New(fmt.Sprintf("unsupported sql type: %d", tp))
	}
}

func TableMetaInfo(sql string) (string, error) {
	tree, err := soar.TiParse(sql, "", "")
	if err != nil {
		return "", err
	}

	jsonString := soar.StmtNode2JSON(sql, "", "")

	node := tree[0]
	switch node.(type) {
	// SetOprStmt represents "union/except/intersect statement"
	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		// DML/DQL: INSERT, SELECT, UPDATE, DELETE
		for _, tableRef := range common.JSONFind(jsonString, "TableRefs") {
			for _, source := range common.JSONFind(tableRef, "Source") {
				table := gjson.Get(source, "Name.O")
				return table.String(), nil
			}
		}
	default:
		return "", errors.New("Not supported sql type: " + sql)
	}

	return "", errors.New("SQL got wrong : " + sql)
}
