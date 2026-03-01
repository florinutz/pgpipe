package testutil

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

// LineCapture is an io.Writer that buffers input and sends each complete line
// to a channel. Useful for capturing adapter output in tests.
type LineCapture struct {
	mu    sync.Mutex
	buf   []byte
	lines chan string
}

// NewLineCapture creates a new LineCapture with a buffered channel.
func NewLineCapture() *LineCapture {
	return &LineCapture{lines: make(chan string, 100)}
}

// Write implements io.Writer. Lines are split on newlines and sent to the
// channel. If the channel is full the line is dropped (non-blocking).
func (c *LineCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf = append(c.buf, p...)
	for {
		idx := bytes.IndexByte(c.buf, '\n')
		if idx < 0 {
			break
		}
		line := string(c.buf[:idx])
		c.buf = c.buf[idx+1:]
		if line != "" {
			select {
			case c.lines <- line:
			default:
			}
		}
	}
	return len(p), nil
}

// WaitLine waits up to timeout for the next line. Calls t.Fatal on timeout.
func (c *LineCapture) WaitLine(t *testing.T, timeout time.Duration) string {
	t.Helper()
	select {
	case line := <-c.lines:
		return line
	case <-time.After(timeout):
		t.Fatal("timed out waiting for output line")
		return ""
	}
}

// Lines returns the underlying channel for raw access.
func (c *LineCapture) Lines() <-chan string {
	return c.lines
}

// Drain discards any buffered lines.
func (c *LineCapture) Drain() {
	for {
		select {
		case <-c.lines:
		default:
			return
		}
	}
}
