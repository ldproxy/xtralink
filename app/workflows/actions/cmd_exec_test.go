package actions

import (
	"bytes"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ldproxy/xtralink/lib/workflows"
)

func TestTokenizeCmd(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  []string
	}{
		{"simple", "cp a b", []string{"cp", "a", "b"}},
		{"double-quoted spaces", `cp "a b" c`, []string{"cp", "a b", "c"}},
		{"single-quoted spaces", `cp 'a b' c`, []string{"cp", "a b", "c"}},
		{"backslash-escaped space", `cp a\ b c`, []string{"cp", "a b", "c"}},
		{"escaped quote inside double quotes", `echo "say \"hi\""`, []string{"echo", `say "hi"`}},
		{"empty", "", nil},
		{"whitespace only", "   ", nil},
		{"shell metacharacters stay literal (no space)", "echo a;b|c", []string{"echo", "a;b|c"}},
		{"shell metacharacters as separate literal token", "echo a ; b", []string{"echo", "a", ";", "b"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := tokenizeCmd(c.input)
			if err != nil {
				t.Fatalf("tokenizeCmd(%q): %v", c.input, err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("tokenizeCmd(%q) = %#v, want %#v", c.input, got, c.want)
			}
		})
	}
}

func TestTokenizeCmd_UnterminatedQuoteIsError(t *testing.T) {
	if _, err := tokenizeCmd(`echo "unterminated`); err == nil {
		t.Fatal("expected an error for an unterminated quote")
	}
}

func TestCmdExecAction_RunsCommand(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	writeFile(t, src, "hello")

	action := &CmdExecAction{}
	result, err := action.Run(&workflows.StepContext{Params: map[string]any{"cmd": "cp " + src + " " + dst}})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected exactly 1 output set, got %+v", result.Outputs)
	}
	assertFileContent(t, dst, "hello")
}

func TestCmdExecAction_QuotedArgumentWithSpaceStaysOneArg(t *testing.T) {
	dir := t.TempDir()
	srcDir := filepath.Join(dir, "src dir")
	writeFile(t, filepath.Join(srcDir, "a.txt"), "a")
	dst := filepath.Join(dir, "dst.txt")

	action := &CmdExecAction{}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"cmd": `cp "` + filepath.Join(srcDir, "a.txt") + `" ` + dst,
	}}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertFileContent(t, dst, "a")
}

func TestCmdExecAction_MissingCmdParamIsError(t *testing.T) {
	action := &CmdExecAction{}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{}}); err == nil {
		t.Fatal("expected an error for a missing cmd parameter")
	}
}

func TestCmdExecAction_CommandFailureReturnsError(t *testing.T) {
	action := &CmdExecAction{}
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{"cmd": "false"}})
	if err == nil {
		t.Fatal("expected an error for a failing command")
	}
	if !strings.Contains(err.Error(), "cmd:exec") {
		t.Errorf("error %q should mention cmd:exec", err.Error())
	}
}

func TestCmdExecAction_UnknownBinaryIsError(t *testing.T) {
	action := &CmdExecAction{}
	if _, err := action.Run(&workflows.StepContext{Params: map[string]any{"cmd": "definitely-not-a-real-binary-xyz"}}); err == nil {
		t.Fatal("expected an error for an unresolvable binary")
	}
}

func TestCmdExecAction_LogsCommandOutputAtDebug(t *testing.T) {
	var buf bytes.Buffer
	action := &CmdExecAction{}

	if _, err := action.Run(&workflows.StepContext{
		Params: map[string]any{"cmd": `sh -c "echo to-stdout; echo to-stderr 1>&2"`},
		Logger: captureLogger(&buf),
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logged := buf.String()
	if !strings.Contains(logged, `"stream":"stdout","message":"to-stdout"`) {
		t.Errorf("stdout line missing from log:\n%s", logged)
	}
	if !strings.Contains(logged, `"stream":"stderr","message":"to-stderr"`) {
		t.Errorf("stderr line missing from log:\n%s", logged)
	}
	if !strings.Contains(logged, "command finished") {
		t.Errorf("completion line missing from log:\n%s", logged)
	}
}

func TestCmdExecAction_LogsNothingBelowDebug(t *testing.T) {
	var buf bytes.Buffer
	action := &CmdExecAction{}

	if _, err := action.Run(&workflows.StepContext{
		Params: map[string]any{"cmd": `sh -c "echo chatty"`},
		Logger: zerolog.New(&buf).Level(zerolog.InfoLevel),
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if buf.Len() != 0 {
		t.Errorf("expected no output at info level, got:\n%s", buf.String())
	}
}

func TestCmdExecAction_FailureErrorCarriesABoundedOutputTail(t *testing.T) {
	action := &CmdExecAction{}

	// Far more output than maxTailBytes, ending in a marker the tail must keep.
	_, err := action.Run(&workflows.StepContext{Params: map[string]any{
		"cmd": `sh -c "for i in $(seq 2000); do echo chatty-failure-output; done; echo LAST-LINE; exit 3"`,
	}})
	if err == nil {
		t.Fatal("expected an error for a failing command")
	}

	message := err.Error()
	if !strings.Contains(message, "LAST-LINE") {
		t.Errorf("error should keep the end of the output, got:\n%s", message)
	}
	if len(message) > 2*maxTailBytes {
		t.Errorf("error is %d bytes, expected it bounded near maxTailBytes (%d)", len(message), maxTailBytes)
	}
}
