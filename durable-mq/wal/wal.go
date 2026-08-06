package wal

import (
	"durable-mq/record"
	"fmt"
	"strconv"
	"strings"
)

// Mode controls how much durability the log actually provides. ModeSync is
// the only mode the system is designed to run in; the other two exist so
// benchmarks can isolate what durability costs by removing it one layer at a
// time, and neither is safe to serve real traffic with.
type Mode int

const (
	// ModeSync fsyncs every append before it is acknowledged. Survives a
	// process crash and a machine crash. The default everywhere.
	ModeSync Mode = iota
	// ModeNoSync appends to the OS page cache and returns without fsyncing.
	// Survives a process crash, loses data on power loss. Benchmark-only:
	// the gap between this and ModeSync is the cost of fsync alone.
	ModeNoSync
	// ModeOff writes nothing at all. Recovery has nothing to replay. This is
	// the no-durability baseline that makes durable-mq directly comparable
	// to push-mq. Benchmark-only.
	ModeOff
)

func (m Mode) String() string {
	switch m {
	case ModeSync:
		return "sync"
	case ModeNoSync:
		return "nosync"
	case ModeOff:
		return "off"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// ParseMode maps a mode name to its Mode. Unknown names are an error rather
// than a fallback — silently degrading to a weaker durability mode is exactly
// the failure this whole package exists to prevent.
func ParseMode(s string) (Mode, error) {
	switch s {
	case "sync":
		return ModeSync, nil
	case "nosync":
		return ModeNoSync, nil
	case "off":
		return ModeOff, nil
	default:
		return ModeSync, fmt.Errorf("unknown wal mode %q (want sync, nosync, or off)", s)
	}
}

// Durable reports whether this mode survives machine loss.
func (m Mode) Durable() bool { return m == ModeSync }

// ParseSize parses a byte size written as "512KB", "128MB", "1GB", or a bare
// byte count. Units are 1024-based, matching how the segment size is spelled
// out in the code. Exists so a benchmark can dial segment size down far
// enough to exercise checkpointing without writing hundreds of megabytes.
func ParseSize(s string) (int64, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(t, "GB"):
		mult, t = 1<<30, strings.TrimSuffix(t, "GB")
	case strings.HasSuffix(t, "MB"):
		mult, t = 1<<20, strings.TrimSuffix(t, "MB")
	case strings.HasSuffix(t, "KB"):
		mult, t = 1<<10, strings.TrimSuffix(t, "KB")
	case strings.HasSuffix(t, "B"):
		t = strings.TrimSuffix(t, "B")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q (want e.g. 1MB, 512KB, or a byte count)", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("size %q must be positive", s)
	}
	return n * mult, nil
}

type WAL struct {
	// In ModeOff sm, w, and r are all nil and offLSN stands in for the
	// writer's counter. Every method that would touch a nil field is
	// reachable only via ShouldCheckpoint, which is hardcoded false in that
	// mode — so checkpointing, and everything it calls, never runs.
	sm *SegmentManager
	w  *Writer
	r  *Reader

	mode   Mode
	offLSN uint64 // ModeOff only; all appends are serialized by Coordinator.mu
}

// Options configures a WAL. The zero value is not usable on its own —
// MaxSegSize has no sensible default at this layer — but Mode and
// CheckpointThreshold both fall back to production values when left unset.
type Options struct {
	MaxSegSize int64 // segment roll threshold in bytes; required
	Mode       Mode  // zero value is ModeSync
	// CheckpointThreshold is how many segments trigger a checkpoint.
	// 0 means DefaultCheckpointThreshold.
	CheckpointThreshold int
}

func Open(dir string, maxSegSize int64) (*WAL, error) {
	return OpenWithOptions(dir, Options{MaxSegSize: maxSegSize})
}

func OpenWithOptions(dir string, opts Options) (*WAL, error) {
	if opts.Mode == ModeOff {
		// Deliberately no directory and no files — an empty WAL dir left
		// behind by a benchmark run would replay as real state next boot.
		return &WAL{mode: opts.Mode}, nil
	}
	sm, err := NewSegmentManagerWithThreshold(dir, opts.CheckpointThreshold)
	if err != nil {
		return nil, err
	}
	// Writer first: Recover() truncates any torn tail before Reader
	// ever sees the segment, so replay never reads corrupt trailing bytes.
	w, err := OpenWriter(sm, opts.MaxSegSize, opts.Mode)
	if err != nil {
		return nil, err
	}
	r, err := OpenReader(sm)
	if err != nil {
		return nil, err
	}
	return &WAL{sm: sm, w: w, r: r, mode: opts.Mode}, nil
}

func (l *WAL) Mode() Mode { return l.mode }

func (l *WAL) Append(rec *record.Record) (uint64, error) {
	if l.mode == ModeOff {
		l.offLSN++
		rec.LSN = l.offLSN
		return rec.LSN, nil
	}
	return l.w.Append(rec)
}

func (l *WAL) ShouldCheckpoint() bool {
	if l.mode == ModeOff {
		return false // nothing was logged, so there is nothing to compact
	}
	return l.sm.ShouldCheckpoint()
}

// ReadUpTo is used for checkpointing and needs its own Reader
func (l *WAL) ReadUpTo(uptoLSN uint64) ([]*record.Record, error) {
	r, err := OpenReader(l.sm)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.ReadUpTo(uptoLSN)
}

func (l *WAL) WriteCheckpointFile(id string, records []*record.Record) (string, string, error) {
	return l.sm.WriteCheckpointFile(id, records)
}

func (l *WAL) DeleteSegmentsBefore(lsn uint64) error {
	return l.sm.DeleteSegmentsBefore(lsn)
}

func (l *WAL) DeleteCheckpointFilesExcept(keepName string) error {
	return l.sm.DeleteCheckpointFilesExcept(keepName)
}

func (l *WAL) ReadAll() ([]*record.Record, error) {
	if l.mode == ModeOff {
		return nil, nil // nothing was ever written, so recovery starts empty
	}
	records, validCkptName, validCkptLSN, err := l.r.ReadAll()
	if err != nil {
		return records, err
	}

	// If multiple ckpt files encountered, delete all except the current one
	l.sm.DeleteCheckpointFilesExcept(validCkptName)

	// Delete unnecessary wal files (covered by ckpt)
	l.sm.DeleteSegmentsBefore(validCkptLSN)

	l.sm.DeleteTmpFiles()
	return records, nil
}

func (l *WAL) Close() error {
	if l.mode == ModeOff {
		return nil
	}
	if err := l.w.Close(); err != nil {
		return err
	}
	return l.r.Close()
}
