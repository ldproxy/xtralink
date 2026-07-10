package cli

import (
	"testing"

	"github.com/alecthomas/kong"
)

// TestFlowRunCmd_RepeatedInputFlagsAccumulate verifies the actual Kong
// parsing behavior of FlowRunCmd.Inputs against real command-line
// arguments - not just Kong's own generic test suite - since sep:"none" is
// what keeps a value like "path=foo/*.zip" from being split on commas, and
// repeated --input flags need to accumulate rather than overwrite.
func TestFlowRunCmd_RepeatedInputFlagsAccumulate(t *testing.T) {
	var cli struct {
		Flow Flow `cmd:""`
	}
	parser, err := kong.New(&cli)
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}

	_, err = parser.Parse([]string{"flow", "run", "check-ldm", "--input", "pkg=foo", "--input", "path=foo/*.zip"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if cli.Flow.Run.Id != "check-ldm" {
		t.Errorf("Id = %q, want check-ldm", cli.Flow.Run.Id)
	}
	want := []string{"pkg=foo", "path=foo/*.zip"}
	if len(cli.Flow.Run.Inputs) != len(want) {
		t.Fatalf("Inputs = %+v, want %+v", cli.Flow.Run.Inputs, want)
	}
	for i, v := range want {
		if cli.Flow.Run.Inputs[i] != v {
			t.Errorf("Inputs[%d] = %q, want %q", i, cli.Flow.Run.Inputs[i], v)
		}
	}
}

func TestFlowRunCmd_NoInputFlagsIsEmpty(t *testing.T) {
	var cli struct {
		Flow Flow `cmd:""`
	}
	parser, err := kong.New(&cli)
	if err != nil {
		t.Fatalf("kong.New: %v", err)
	}

	if _, err := parser.Parse([]string{"flow", "run", "check-ldm"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cli.Flow.Run.Inputs) != 0 {
		t.Errorf("Inputs = %+v, want empty", cli.Flow.Run.Inputs)
	}
}
