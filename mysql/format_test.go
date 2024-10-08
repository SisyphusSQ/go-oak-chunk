package mysql

import (
	"fmt"
	"testing"
)

func TestBuildWhereClause(t *testing.T) {
	unqKey := &UnqKeys{
		UniqueKeyColumns: []string{"id", "c", "pad"},
		CountColumns:     0,
		UniqueKeyTypes:   []byte{1, 1, 1},
		IsNull:           []bool{false, false, false},
	}

	m := BuildSelectWhereClause(unqKey)
	for k, v := range m {
		println(k + " : " + v)
	}
}

func TestBuildDeleteWhereClause(t *testing.T) {
	unqKey := &UnqKeys{
		UniqueKeyColumns: []string{"id", "c", "pad"},
		CountColumns:     0,
		UniqueKeyTypes:   []byte{1, 1, 1},
		IsNull:           []bool{false, true, false},
	}

	println(BuildBulkExecWhereClause(unqKey))
}

func TestDel(t *testing.T) {
	unqKey := &UnqKeys{
		UniqueKeyColumns: []string{"id", "c", "pad"},
		CountColumns:     0,
		UniqueKeyTypes:   []byte{1, 1, 1},
		IsNull:           []bool{false, false, false},
	}

	conditions := BuildSelectWhereClause(unqKey)

	// build delete stmt
	var deleteWhere string
	chunkSize := 50
	if chunkSize == 1 {
		// index can't be not unique
		deleteWhere = BuildBulkExecWhereClause(unqKey)
	} else {
		deleteWhere = fmt.Sprintf(" AND (%s) AND (%s) limit %d", conditions[">="], conditions["<="], chunkSize)
	}
	println(deleteWhere)
}
