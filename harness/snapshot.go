package harness

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// StatePayload is an immutable snapshot of an operational component state.
//
// Data is always copied when a payload enters or leaves a context. This
// prevents the failed context and its replacement from sharing mutable
// backing memory.
type StatePayload struct {
	Version  uint64
	Captured time.Time
	Data     []byte
}

// ErrNoStateSnapshot indicates that no last-known-good state has been
// checkpointed for the context.
var ErrNoStateSnapshot = errors.New("no state snapshot available")

// SnapshotProvider materializes the current last-known-good operational state.
//
// The provider is ALWAYS invoked without the SpatioTemporalContext mutex held.
// Implementations may therefore safely acquire locks belonging to the
// application component.
type SnapshotProvider func() ([]byte, error)

// RestoreHandler can be used by application code to restore a StatePayload
// into a running component.
//
// The harness itself does not invoke RestoreHandler while holding any
// internal context lock.
type RestoreHandler func(StatePayload) error

// newStatePayload constructs an immutable payload by copying the supplied
// byte slice.
func newStatePayload(version uint64, data []byte) StatePayload {
	return StatePayload{
		Version:  version,
		Captured: time.Now().UTC(),
		Data:     append([]byte(nil), data...),
	}
}

// clone creates a completely independent copy of a StatePayload.
func (p StatePayload) clone() StatePayload {
	return StatePayload{
		Version:  p.Version,
		Captured: p.Captured,
		Data:     append([]byte(nil), p.Data...),
	}
}

// MarshalJSONState is a convenience helper for components whose operational
// state can be represented as JSON.
//
// It is intentionally independent of SpatioTemporalContext so callers can
// construct a checkpoint before passing it to UpdateStateSnapshot.
func MarshalJSONState(v any) ([]byte, error) {
	if v == nil {
		return nil, errors.New("cannot snapshot nil state")
	}

	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal state snapshot: %w", err)
	}

	return data, nil
}
