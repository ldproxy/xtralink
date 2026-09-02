package actions

import (
	"bytes"
	"sync"

	"github.com/rs/zerolog"
)

const (
	// maxLoggedLine caps a single logged output line, so one command
	// emitting a megabyte without a newline (a progress bar, a minified
	// blob) cannot flood the log with it.
	maxLoggedLine = 2 << 10
	// maxTailLines caps how much output an error message carries. A failing
	// command's last few lines are what says why it failed; everything
	// before that is available at debug level, one line at a time.
	maxTailLines = 10
	// maxTailBytes is the byte backstop behind maxTailLines, for output that
	// arrives as a few enormous lines rather than many small ones.
	maxTailBytes = 4 << 10
)

// logWriter turns a command's output stream into one log line per output
// line: it buffers whatever arrives without a trailing newline until the
// rest of that line shows up, so a line split across two Write calls is
// still logged once, whole.
//
// Every write is additionally teed into tail, which is what the error
// message reports if the command fails.
type logWriter struct {
	logger  zerolog.Logger
	stream  string
	tail    *tailBuffer
	partial []byte
}

func newLogWriter(logger zerolog.Logger, stream string, tail *tailBuffer) *logWriter {
	return &logWriter{logger: logger, stream: stream, tail: tail}
}

func (w *logWriter) Write(p []byte) (int, error) {
	w.tail.Write(p)

	rest := append(w.partial, p...)
	for {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			break
		}
		w.log(rest[:i])
		rest = rest[i+1:]
	}
	w.partial = append(w.partial[:0], rest...)

	return len(p), nil
}

// Flush logs a trailing line that never got its newline. Call it once the
// command has exited and no more output can arrive.
func (w *logWriter) Flush() {
	if len(w.partial) > 0 {
		w.log(w.partial)
		w.partial = w.partial[:0]
	}
}

func (w *logWriter) log(line []byte) {
	line = bytes.TrimRight(line, "\r")
	if len(line) > maxLoggedLine {
		line = line[:maxLoggedLine]
	}
	w.logger.Debug().Str("stream", w.stream).Msg(string(line))
}

// tailBuffer keeps the end of what was written to it and nothing else,
// bounding what a failing command can put into an error message. Both of a
// command's output streams share one, preserving the interleaved ordering
// exec.Cmd.CombinedOutput would have produced - which is why it locks:
// os/exec copies stdout and stderr on separate goroutines whenever they
// are distinct non-*os.File writers.
type tailBuffer struct {
	mu        sync.Mutex
	buf       []byte
	limit     int
	truncated bool
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{limit: limit}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.buf = append(t.buf, p...)
	if len(t.buf) > t.limit {
		t.buf = append(t.buf[:0], t.buf[len(t.buf)-t.limit:]...)
		t.truncated = true
	}
	return len(p), nil
}

// String returns the tail as at most maxTailLines lines. If the byte limit
// cut into a line, that fragment is dropped so the result always starts
// where a line does.
func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	lines := bytes.Split(bytes.TrimRight(t.buf, "\n"), []byte("\n"))
	if t.truncated && len(lines) > 1 {
		lines = lines[1:]
	}
	if len(lines) > maxTailLines {
		lines = lines[len(lines)-maxTailLines:]
	}
	return string(bytes.Join(lines, []byte("\n")))
}
