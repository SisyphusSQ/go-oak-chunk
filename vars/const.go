package vars

// query sql
const (
	TableInfoSQL = "show create table %s"

	TableExistsSQL = `
        SELECT COUNT(*) AS count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA= ?
            AND TABLE_NAME= ?`

	LockTableSQL = "LOCK TABLES %s.%s READ"

	UnlockTableSQL = "UNLOCK TABLES"

	FirstSQL = "select /*!40001 SQL_NO_CACHE */ %s from %s where %s"
)

const (
	ConstraintNoConstraint int = iota
	ConstraintPrimaryKey
	ConstraintKey
	ConstraintIndex
	ConstraintUniq
	ConstraintUniqKey
	ConstraintUniqIndex
	ConstraintForeignKey
	ConstraintFulltext
	ConstraintCheck
)

const (
	DEBUG uint = iota
	INFO
	WARN
	ERROR
	FATAL
)

const LagThreshold int64 = -1

const Billion = 1000000000
