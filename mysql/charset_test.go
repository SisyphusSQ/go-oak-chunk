package mysql

import (
	"strings"
	"testing"

	"github.com/SisyphusSQ/go-oak-chunk/v3/conf"
	"github.com/SisyphusSQ/go-oak-chunk/v3/vars"
)

// Mirrors the reported SHOW CREATE TABLE, including mixed column charsets.
const utf16SendRecordDDL = "CREATE TABLE `ex_edm_send_record` (" +
	"`id` int(11) NOT NULL AUTO_INCREMENT," +
	"`status` int(11) DEFAULT NULL COMMENT '-3重复发送 -2白名单用户 -1取消订阅 0待发送 1发送成功'," +
	"`partner_id` int(11) DEFAULT NULL COMMENT '合作方id'," +
	"`lang` varchar(11) DEFAULT NULL COMMENT '语言'," +
	"`mode` varchar(30) DEFAULT NULL COMMENT '模式'," +
	"`template_instance_id` int(11) DEFAULT NULL COMMENT '实例id'," +
	"`subject` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '主题'," +
	"`receiver` varchar(100) CHARACTER SET utf16 DEFAULT NULL COMMENT '接受者'," +
	"`email_encryption` varchar(255) DEFAULT NULL COMMENT '邮箱加密'," +
	"`statistics_id` varchar(255) DEFAULT NULL COMMENT '统计字段'," +
	"`create_time` datetime DEFAULT NULL,`send_time` datetime DEFAULT NULL,`expire_time` datetime DEFAULT NULL," +
	"`msg` varchar(200) DEFAULT NULL," +
	"`mail_type` tinyint(4) DEFAULT NULL COMMENT '邮件类型 0：营销类邮件1：功能类 2服务类'," +
	"`mail_sub_type` tinyint(4) DEFAULT NULL COMMENT '邮件子类型'," +
	"PRIMARY KEY (`id`),KEY `user_id_index` (`partner_id`,`lang`,`template_instance_id`)," +
	"KEY `create_time_statistics` (`create_time`,`statistics_id`)" +
	") ENGINE=InnoDB AUTO_INCREMENT=436135712 DEFAULT CHARSET=utf8 COMMENT='【发送记录表】'"

func TestWriterGetInfoFromTable_UTF16(t *testing.T) {
	for _, declaration := range []string{"utf16", "UTF16", "utf16 COLLATE utf16_general_ci", "utf16 COLLATE utf16_bin"} {
		t.Run(declaration, func(t *testing.T) {
			t.Parallel()
			ddl := strings.Replace(utf16SendRecordDDL, "CHARACTER SET utf16", "CHARACTER SET "+declaration, 1)
			db, _ := newGetInfoTestDB(t, getInfoPlan{createTableDDL: ddl})
			w := newWriterGetInfoForTest(db)
			w.Database = "intl_edm_v2"
			w.Table = "ex_edm_send_record"
			w.ExecuteSQL = "DELETE FROM intl_edm_v2.ex_edm_send_record WHERE id > 0"
			if err := w.getInfoFromTable(&conf.Config{ForceChunkingColumn: "id"}); err != nil {
				t.Fatal(err)
			}
			assertUniqueKeyColumns(t, w.unqKeys, vars.ConstraintPrimaryKey, []string{"id"})
			if w.ExecuteSQL != "DELETE FROM `intl_edm_v2`.`ex_edm_send_record` WHERE (`id`>0)" {
				t.Fatalf("unexpected DML: %s", w.ExecuteSQL)
			}
		})
	}
}

func TestWriterGetInfoFromTable_UnknownCharsetRejected(t *testing.T) {
	db, _ := newGetInfoTestDB(t, getInfoPlan{createTableDDL: strings.Replace(utf16SendRecordDDL, "CHARACTER SET utf16", "CHARACTER SET not_a_charset", 1)})
	w := newWriterGetInfoForTest(db)
	if err := w.getInfoFromTable(&conf.Config{}); err == nil || !strings.Contains(err.Error(), "Unknown character set") {
		t.Fatalf("expected unknown charset rejection, got %v", err)
	}
}
