package mysql

import (
	"fmt"
	"strings"

	"go-oak-chunk/v2/utils/string_utils"
)

const (
	Quota = "`"
	Value = "?"
)

// BuildSelectWhereClause use pt-archiver logic
func BuildSelectWhereClause(unqKeys *UnqKeys) map[string]string {
	conditionMap := make(map[string]string, 4)
	for _, cmp := range []string{"<", "<=", ">", ">="} {
		clauses := make([]string, 0)
		cmpWithoutEq := strings.Replace(cmp, "=", "", -1)
		for i, col := range unqKeys.UniqueKeyColumns {
			clause := make([]string, 0)
			for j := 0; j <= i-1; j++ {
				var where string
				key := Quota + unqKeys.UniqueKeyColumns[j] + Quota
				if unqKeys.IsNull[j] {
					where = fmt.Sprintf("((%s IS NULL AND %s IS NULL) OR (%s = %s))", Value, key, key, Value)
				} else {
					where = fmt.Sprintf("%s = %s", key, Value)
				}
				clause = append(clause, where)
			}

			var where string
			key := Quota + col + Quota
			isEnd := i == len(unqKeys.UniqueKeyColumns)-1
			if unqKeys.IsNull[i] {
				if string_utils.ContainsAny(cmp, []string{"<=", ">="}) && isEnd {
					where = fmt.Sprintf("(%s IS NULL OR %s %s %s)", Value, key, cmp, Value)
					clause = append(clause, where)
				} else if string_utils.ContainsAny(cmp, []string{">", ">="}) {
					where = fmt.Sprintf("((%s IS NULL AND %s IS NOT NULL) OR (%s %s %s))", Value, key, key, cmpWithoutEq, Value)
					clause = append(clause, where)
				} else {
					where = fmt.Sprintf("((%s IS NOT NULL AND %s IS NULL) OR (%s %s %s))", Value, key, key, cmpWithoutEq, Value)
					clauses = append(clauses, where)
				}
			} else {
				if string_utils.ContainsAny(cmp, []string{"<=", ">="}) && isEnd {
					where = fmt.Sprintf("%s %s %s", key, cmp, Value)
					clause = append(clause, where)
				} else {
					where = fmt.Sprintf("%s %s %s", key, cmpWithoutEq, Value)
					clause = append(clause, where)
				}
			}

			if len(clause) != 0 {
				elem := "(" + strings.Join(clause, " AND ") + ")"
				clauses = append(clauses, elem)
			}
		}
		conditionMap[cmp] = "(" + strings.Join(clauses, " OR ") + ")"
	}

	return conditionMap
}

func BuildBulkExecWhereClause(unqKeys *UnqKeys) string {
	clauses := make([]string, 0)
	for i, col := range unqKeys.UniqueKeyColumns {
		key := Quota + col + Quota
		if unqKeys.IsNull[i] {
			clauses = append(clauses, fmt.Sprintf("((%s IS NULL AND %s IS NULL) OR (%s = %s))", Value, key, key, Value))
		} else {
			clauses = append(clauses, fmt.Sprintf("%s = %s", key, Value))
		}
	}
	return " AND " + "(" + strings.Join(clauses, " AND ") + ")"
}
