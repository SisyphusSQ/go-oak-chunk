package mysql

import "github.com/pingcap/tidb/parser/charset"

func init() {
	// TiDB knows MySQL's utf16 definition and collations but rejects it as
	// unsupported. Register that definition so SHOW CREATE TABLE can be parsed
	// when even a non-key column uses utf16. This is parser metadata only, not
	// an encoding implementation; character comparisons remain server-side.
	// Registration affects the process-wide parser registry. Do it during
	// package initialization, before concurrent CLI/SDK parsing can begin.
	if info, err := charset.GetCharsetInfo(charset.CharsetUTF16); err != nil && info != nil {
		charset.AddCharset(info)
	}
}
