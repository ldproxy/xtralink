package actions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// captureLogger returns a Debug-level logger writing JSON lines into buf.
func captureLogger(buf *bytes.Buffer) zerolog.Logger {
	return zerolog.New(buf).Level(zerolog.DebugLevel)
}

// loggedMessages extracts the "message" field of every captured log line.
func loggedMessages(t *testing.T, buf *bytes.Buffer) []string {
	t.Helper()

	var messages []string
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line %q is not JSON: %v", line, err)
		}
		message, _ := entry["message"].(string)
		messages = append(messages, message)
	}
	return messages
}

func TestLogWriter_OneLogLinePerOutputLine(t *testing.T) {
	var buf bytes.Buffer
	w := newLogWriter(captureLogger(&buf), "stdout", newTailBuffer(maxTailBytes))

	if _, err := w.Write([]byte("first\nsecond\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	w.Flush()

	got := loggedMessages(t, &buf)
	if want := []string{"first", "second"}; !reflect.DeepEqual(got, want) {
		t.Errorf("logged %#v, want %#v", got, want)
	}
}

func TestLogWriter_LineSplitAcrossWritesIsLoggedOnce(t *testing.T) {
	var buf bytes.Buffer
	w := newLogWriter(captureLogger(&buf), "stdout", newTailBuffer(maxTailBytes))

	for _, chunk := range []string{"one li", "ne hol", "ding together\n"} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write(%q): %v", chunk, err)
		}
	}
	w.Flush()

	got := loggedMessages(t, &buf)
	if want := []string{"one line holding together"}; !reflect.DeepEqual(got, want) {
		t.Errorf("logged %#v, want %#v", got, want)
	}
}

func TestLogWriter_TrailingLineWithoutNewlineIsLoggedOnFlush(t *testing.T) {
	var buf bytes.Buffer
	w := newLogWriter(captureLogger(&buf), "stdout", newTailBuffer(maxTailBytes))

	if _, err := w.Write([]byte("no trailing newline")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if messages := loggedMessages(t, &buf); len(messages) != 0 {
		t.Fatalf("expected nothing logged before Flush, got %#v", messages)
	}

	w.Flush()

	got := loggedMessages(t, &buf)
	if want := []string{"no trailing newline"}; !reflect.DeepEqual(got, want) {
		t.Errorf("logged %#v, want %#v", got, want)
	}
}

func TestLogWriter_TagsTheStreamItBelongsTo(t *testing.T) {
	var buf bytes.Buffer
	logger := captureLogger(&buf)
	tail := newTailBuffer(maxTailBytes)

	newLogWriter(logger, "stdout", tail).Write([]byte("out\n"))
	newLogWriter(logger, "stderr", tail).Write([]byte("err\n"))

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 log lines, got %d: %s", len(lines), buf.String())
	}
	if !strings.Contains(lines[0], `"stream":"stdout"`) {
		t.Errorf("first line %q should be tagged stdout", lines[0])
	}
	if !strings.Contains(lines[1], `"stream":"stderr"`) {
		t.Errorf("second line %q should be tagged stderr", lines[1])
	}
}

func TestLogWriter_OverlongLineIsTruncated(t *testing.T) {
	var buf bytes.Buffer
	w := newLogWriter(captureLogger(&buf), "stdout", newTailBuffer(maxTailBytes))

	if _, err := w.Write(append(bytes.Repeat([]byte("x"), maxLoggedLine+500), '\n')); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := loggedMessages(t, &buf)
	if len(got) != 1 {
		t.Fatalf("expected 1 log line, got %#v", got)
	}
	if len(got[0]) != maxLoggedLine {
		t.Errorf("logged line is %d bytes, want it truncated to %d", len(got[0]), maxLoggedLine)
	}
}

func TestLogWriter_StripsCarriageReturnFromCRLFOutput(t *testing.T) {
	var buf bytes.Buffer
	w := newLogWriter(captureLogger(&buf), "stdout", newTailBuffer(maxTailBytes))

	if _, err := w.Write([]byte("windows line\r\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := loggedMessages(t, &buf)
	if want := []string{"windows line"}; !reflect.DeepEqual(got, want) {
		t.Errorf("logged %#v, want %#v", got, want)
	}
}

func TestTailBuffer_KeepsOnlyTheLastBytes(t *testing.T) {
	tail := newTailBuffer(4)

	for _, chunk := range []string{"abc", "de", "fgh"} {
		if _, err := tail.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write(%q): %v", chunk, err)
		}
	}

	if got := tail.String(); got != "efgh" {
		t.Errorf("tail = %q, want %q", got, "efgh")
	}
}

func TestTailBuffer_KeepsOnlyTheLastLines(t *testing.T) {
	tail := newTailBuffer(maxTailBytes)

	for i := 0; i < maxTailLines+5; i++ {
		if _, err := tail.Write([]byte(fmt.Sprintf("line %d\n", i))); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	lines := strings.Split(tail.String(), "\n")
	if len(lines) != maxTailLines {
		t.Fatalf("tail has %d lines, want %d:\n%s", len(lines), maxTailLines, tail.String())
	}
	if lines[0] != "line 5" {
		t.Errorf("tail starts at %q, want \"line 5\"", lines[0])
	}
	if last := lines[len(lines)-1]; last != fmt.Sprintf("line %d", maxTailLines+4) {
		t.Errorf("tail ends at %q, want the final line", last)
	}
}

func TestTailBuffer_DropsALeadingPartialLine(t *testing.T) {
	tail := newTailBuffer(8)
	tail.Write([]byte("cut-here\nwhole\n"))

	if got := tail.String(); got != "whole" {
		t.Errorf("tail = %q, want %q - a partial first line should be dropped", got, "whole")
	}
}

func TestTailBuffer_ShorterThanLimitIsKeptWhole(t *testing.T) {
	tail := newTailBuffer(maxTailBytes)
	tail.Write([]byte("short"))

	if got := tail.String(); got != "short" {
		t.Errorf("tail = %q, want %q", got, "short")
	}
}

func TestTailBuffer_SharedByBothStreamsInterleavesThem(t *testing.T) {
	tail := newTailBuffer(maxTailBytes)
	logger := zerolog.Nop()

	newLogWriter(logger, "stdout", tail).Write([]byte("out\n"))
	newLogWriter(logger, "stderr", tail).Write([]byte("err\n"))

	if got := tail.String(); got != "out\nerr" {
		t.Errorf("tail = %q, want %q", got, "out\nerr")
	}
}
