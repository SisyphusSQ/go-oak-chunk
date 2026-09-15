package mysql

import (
	"strings"
	"testing"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/vars"
)

// Synthetic fixture covering a non-key utf16 column and mixed charsets.
const utf16MetadataDDL = "CREATE TABLE `t1` (" +
	"`id` int NOT NULL AUTO_INCREMENT," +
	"`label` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL," +
	"`value` varchar(100) CHARACTER SET utf16 DEFAULT NULL," +
	"PRIMARY KEY (`id`), KEY `idx_label` (`label`)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8"

func TestWriterGetInfoFromTable_UTF16(t *testing.T) {
	for _, declaration := range []string{"utf16", "UTF16", "utf16 COLLATE utf16_general_ci", "utf16 COLLATE utf16_bin"} {
		t.Run(declaration, func(t *testing.T) {
			t.Parallel()
			ddl := strings.Replace(utf16MetadataDDL, "CHARACTER SET utf16", "CHARACTER SET "+declaration, 1)
			db, _ := newGetInfoTestDB(t, getInfoPlan{createTableDDL: ddl})
			w := newWriterGetInfoForTest(db)
			w.Database = "test_db"
			w.Table = "t1"
			w.ExecuteSQL = "DELETE FROM test_db.t1 WHERE id > 0"
			if err := w.getInfoFromTable(&conf.Config{ForceChunkingColumn: "id"}); err != nil {
				t.Fatal(err)
			}
			assertUniqueKeyColumns(t, w.unqKeys, vars.ConstraintPrimaryKey, []string{"id"})
			if w.ExecuteSQL != "DELETE FROM `test_db`.`t1` WHERE (`id`>0)" {
				t.Fatalf("unexpected DML: %s", w.ExecuteSQL)
			}
		})
	}
}

func TestWriterGetInfoFromTable_UnknownCharsetRejected(t *testing.T) {
	db, _ := newGetInfoTestDB(t, getInfoPlan{createTableDDL: strings.Replace(utf16MetadataDDL, "CHARACTER SET utf16", "CHARACTER SET not_a_charset", 1)})
	w := newWriterGetInfoForTest(db)
	if err := w.getInfoFromTable(&conf.Config{}); err == nil || !strings.Contains(err.Error(), "Unknown character set") {
		t.Fatalf("expected unknown charset rejection, got %v", err)
	}
}
