//go:build unix

package recorder

import "syscall"

const recordingNoFollow = syscall.O_NOFOLLOW
