package actions

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

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
