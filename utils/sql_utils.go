package utils

import (
	"fmt"
	"strings"
)

const Quota = "'"
const Equal = "="
const LessThen = "<"
const GreatThen = ">"

// GetMultipleColumnsNonEqualityComparison Given a list of columns and a list of values (of same length)
// produce a 'less than' or 'greater than' (optionally 'or Equal') SQL equasion, by splitting into multiple conditions.
// An example result may look like:
// (col1 < val1) OR
// ((col1 = val1) AND (col2 < val2)) OR
// ((col1 = val1) AND (col2 = val2) AND (col3 < val3)) OR
// ((col1 = val1) AND (col2 = val2) AND (col3 = val3)))
// Which stands for (col1, col2, col3) <= (val1, val2, val3).
// The latter being simple in representation, however MySQL does not utilize keys
// properly with this form of condition, hence the splitting into multiple conditions.
func GetMultipleColumnsNonEqualityComparison(tableName string, columnNames []string, columnValues []int64, comparisonSign string, includeEquality bool) string {
	comparisons := make([]string, 0)
	for i := 0; i < len(columnNames); i++ {
		var comparison string
		equalitiesComparison := GetMultipleColumnsEquality(tableName, columnNames[0:i], columnValues[0:i])
		rangeComparison := GetValueComparison(tableName, columnNames[i], columnValues[i], comparisonSign)

		if equalitiesComparison == "()" {
			comparison = rangeComparison
		} else {
			comparison = fmt.Sprintf("(%s AND %s)", equalitiesComparison, rangeComparison)
		}
		comparisons = append(comparisons, comparison)
	}

	if includeEquality {
		equalitiesComparison := GetMultipleColumnsEquality(tableName, columnNames, columnValues)
		comparisons = append(comparisons, equalitiesComparison)
	}

	return fmt.Sprintf("(%s)", strings.Join(comparisons, " OR "))
}

// GetMultipleColumnsEquality Given a list of columns and a list of values (of same length),
// produce an SQL equality of the form:
// ((col1 = val1) AND (col2 = val2) AND...)
func GetMultipleColumnsEquality(tableName string, columnNames []string, columnValues []int64) string {
	equalities := make([]string, 0)
	for i := 0; i < len(columnNames); i++ {
		equality := GetValueComparison(tableName, columnNames[i], columnValues[i], Equal)
		equalities = append(equalities, equality)
	}
	return fmt.Sprintf("(%s)", strings.Join(equalities, " AND "))
}

// GetValueComparison Given a column, value and comparison sign, return the SQL comparison of the two.
// e.g. 'id', 7, '<'
// results with (id < 7)
func GetValueComparison(tableName string, columnName string, columnValue int64, comparisonSign string) string {
	return fmt.Sprintf("(%s %s %d)", tableName+"."+columnName, comparisonSign, columnValue)
}
