// Package payload is the wire convention between whatever is publishing and
// whatever is receiving in the e2e harness.
//
// dist-mq delivers the message body and nothing else — no id, no headers — so
// if a test wants to know which message arrived, the answer has to be inside
// the bytes it published. A token on the first line does that while leaving
// the rest free for padding, so payload size stays an independent knob.
package payload

import "bytes"

// Encode returns token, a newline, and enough filler to reach size bytes.
// A size smaller than the token leaves the token intact and skips the padding:
// identity is not negotiable, payload size is.
func Encode(token string, size int) []byte {
	head := make([]byte, 0, max(size, len(token)+1))
	head = append(head, token...)
	head = append(head, '\n')
	if pad := size - len(head); pad > 0 {
		head = append(head, bytes.Repeat([]byte("x"), pad)...)
	}
	return head
}

// Token reads back what Encode wrote. A body with no newline is treated as a
// bare token, so a payload published by hand still identifies itself.
func Token(body []byte) string {
	if i := bytes.IndexByte(body, '\n'); i >= 0 {
		return string(body[:i])
	}
	return string(body)
}
