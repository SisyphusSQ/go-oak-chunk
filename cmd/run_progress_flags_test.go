package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
)

func TestRunConfigNoProgressBar(t *testing.T) {
	ensureRunInit()
	configFile := filepath.Join(t.TempDir(), "goc.toml")
	if err := os.WriteFile(configFile, []byte("execute_query = 'DELETE FROM t WHERE id > 0'\ndatabase = 'test'\nprint_progress = true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{"default", nil, false},
		{"enabled", []string{"--print-progress"}, true},
		{"disabled", []string{"--no-progress-bar"}, false},
		{"override_cli", []string{"--print-progress", "--no-progress-bar"}, false},
		{"override_cli_reverse_order", []string{"--no-progress-bar", "--print-progress"}, false},
		{"explicit_false", []string{"--print-progress", "--no-progress-bar=false"}, true},
		{"config_enabled", []string{"-c", configFile}, true},
		{"override_config", []string{"-c", configFile, "--no-progress-bar"}, false},
		{"config_explicit_false", []string{"-c", configFile, "--no-progress-bar=false"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// CLI bindings are package globals; isolate each case from other flag tests.
			runCmd.Flags().VisitAll(func(f *pflag.Flag) {
				value, changed := f.Value.String(), f.Changed
				t.Cleanup(func() {
					_ = f.Value.Set(value)
					f.Changed = changed
				})
				if err := f.Value.Set(f.DefValue); err != nil {
					t.Fatal(err)
				}
				f.Changed = false
			})
			args := append([]string{"-e", "DELETE FROM t WHERE id > 0", "-d", "test"}, tt.args...)
			if err := runCmd.ParseFlags(args); err != nil {
				t.Fatal(err)
			}
			config, err := loadRunConfig()
			if err != nil {
				t.Fatal(err)
			}
			if config.PrintProgress != tt.want {
				t.Fatalf("PrintProgress = %v, want %v", config.PrintProgress, tt.want)
			}
		})
	}
}
