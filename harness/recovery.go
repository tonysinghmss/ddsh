package harness

import (
	"context"
	"errors"
	"fmt"
)

// StateRestorer is an optional component capability used during replacement
// recovery. The handler is invoked without harness locks held.
type StateRestorer interface {
	RestoreState(StatePayload) error
}

var (
	ErrReplacementNameMismatch = errors.New("replacement component name mismatch")
	ErrReplacementUnavailable  = errors.New("replacement target is unavailable")
)

// RecoverWithReplacement replaces the runtime component behind failedName
// while preserving its logical DAG identity. The replacement receives the
// failed context's last-known-good snapshot before the graph transition is
// reconciled.
//
// Graph replacement and dependency reconciliation are serialized as one graph
// transition. Downstream callbacks are always reached through the dependency
// planner rather than direct recovery callback chains.
func (dt *DependencyTracker) RecoverWithReplacement(parentCtx context.Context, failedName string, replacement Component) error {
	if replacement == nil {
		return fmt.Errorf("%w: nil replacement", ErrReplacementUnavailable)
	}
	if replacement.Name() != failedName {
		return fmt.Errorf("%w: failed=%q replacement=%q", ErrReplacementNameMismatch, failedName, replacement.Name())
	}

	dt.clearError()
	node, ok := dt.graph.Node(failedName)
	if !ok || node.Component == nil {
		err := fmt.Errorf("%w: %q", ErrReplacementUnavailable, failedName)
		dt.recordError(err)
		return err
	}
	failedCtx := node.Context
	if failedCtx == nil {
		err := fmt.Errorf("%w: no failed context for %q", ErrReplacementUnavailable, failedName)
		dt.recordError(err)
		return err
	}

	payload, err := failedCtx.StateSnapshot()
	if err != nil {
		dt.recordError(err)
		return err
	}
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	if parent := failedCtx.ParentST(); parent != nil {
		parentCtx = parent
	}

	replacementCtx := NewSTContext(parentCtx, failedName, dt.onRecovery)
	replacementCtx.RestoreStateSnapshot(payload)

	// Application restore code executes before graph mutation and without any
	// tracker, graph, or context lock held.
	if restorer, ok := replacement.(StateRestorer); ok {
		if err := restorer.RestoreState(payload.clone()); err != nil {
			replacementCtx.Rollback()
			dt.recordError(err)
			return fmt.Errorf("restore replacement %q: %w", failedName, err)
		}
	}

	dt.graph.planMu.Lock()
	defer dt.graph.planMu.Unlock()

	plan, err := dt.graph.replaceNodeRuntimeUnlocked(failedName, replacement, replacementCtx)
	if err != nil {
		replacementCtx.Rollback()
		dt.recordError(err)
		return err
	}
	if err := dt.graph.executePlanLocked(plan, DependencyExecutionOptions{ParentContext: parentCtx, Recovery: dt.onRecovery}); err != nil {
		dt.recordError(err)
		return err
	}
	return nil
}
