package actions

import (
	"fmt"
	"os/exec"
	"strings"
	"unicode"

	"github.com/ldproxy/xtralink/lib/workflows"
)

// CmdExecAction implements "cmd:exec": runs an arbitrary command. The
// resolved "cmd" string (template placeholders like ${params...}/
// ${outputs...} are already substituted by the engine before Run is called)
// is tokenized by tokenizeCmd and executed directly via exec.Command -
// deliberately NOT via a shell (e.g. "sh -c"). A workflow author's cmd:
// template is trusted, but an interpolated value coming from a caller (a
// --input param) is not: passing the resolved string to a real shell would
// let shell metacharacters in that value (e.g. ";", "|") break out of their
// intended argument position and inject additional commands. Executing the
// tokenized argv directly closes that off entirely.
type CmdExecAction struct{}

func (a *CmdExecAction) Type() string { return "cmd:exec" }

func (a *CmdExecAction) Run(ctx *workflows.StepContext) (workflows.StepResult, error) {
	cmdStr, ok := ctx.Params["cmd"].(string)
	if !ok || strings.TrimSpace(cmdStr) == "" {
		return workflows.StepResult{}, fmt.Errorf(`cmd:exec: "cmd" parameter is required`)
	}

	argv, err := tokenizeCmd(cmdStr)
	if err != nil {
		return workflows.StepResult{}, fmt.Errorf("cmd:exec: %w", err)
	}
	if len(argv) == 0 {
		return workflows.StepResult{}, fmt.Errorf("cmd:exec: %q resolved to an empty command", cmdStr)
	}

	out, err := exec.Command(argv[0], argv[1:]...).CombinedOutput()
	if err != nil {
		return workflows.StepResult{}, fmt.Errorf("cmd:exec: command %q failed: %w (output: %s)", cmdStr, err, out)
	}

	return workflows.Success(), nil
}

// tokenizeCmd splits cmd into argv the way a shell's word-splitting would,
// honoring single/double quotes and backslash-escapes so that a quoted
// argument containing spaces stays one token - but WITHOUT interpreting any
// other shell syntax (no $VAR expansion, no ;/|/&, no globbing). That
// restriction is the whole point: it's the safety property "no shell" is
// meant to buy.
func tokenizeCmd(cmd string) ([]string, error) {
	var tokens []string
	var cur strings.Builder
	inToken := false
	var quote rune

	runes := []rune(cmd)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		switch {
		case quote != 0:
			switch {
			case c == quote:
				quote = 0
			case c == '\\' && quote == '"' && i+1 < len(runes) && (runes[i+1] == '"' || runes[i+1] == '\\'):
				cur.WriteRune(runes[i+1])
				i++
			default:
				cur.WriteRune(c)
			}
			inToken = true
		case c == '\'' || c == '"':
			quote = c
			inToken = true
		case c == '\\' && i+1 < len(runes):
			cur.WriteRune(runes[i+1])
			i++
			inToken = true
		case unicode.IsSpace(c):
			if inToken {
				tokens = append(tokens, cur.String())
				cur.Reset()
				inToken = false
			}
		default:
			cur.WriteRune(c)
			inToken = true
		}
	}

	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote in command %q", cmd)
	}
	if inToken {
		tokens = append(tokens, cur.String())
	}
	return tokens, nil
}
