package engine

import (
	"context"
	"database/sql"
	"encoding/json"
)

// replaceDerived runs inside the phase-2 transaction. It first invalidates all
// derived structures of every impacted component and then rebuilds views,
// reference relations and the affected set atomically, so readers can never
// observe a half-rebuilt dependency graph.
func (e *Engine) replaceDerived(ctx context.Context, tx *sql.Tx,
	st *materializedState, views map[string]*ComponentView,
	triggerKeys []string, version int64) error {

	impacted := impactedComponents(st, triggerKeys)

	// Cascade invalidation: drop every derived row of impacted components.
	for id := range impacted {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM views WHERE component_id=?`, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM view_refs WHERE component_id=?`, id); err != nil {
			return err
		}
	}

	// Atomic rebuild of all views and their reference relations.
	for id, view := range views {
		raw, err := json.Marshal(view)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO views(component_id, version, digest, view_json)
			VALUES(?,?,?,?)
			ON CONFLICT(component_id) DO UPDATE SET
			  version=excluded.version, digest=excluded.digest,
			  view_json=excluded.view_json`,
			id, view.Version, view.Digest, string(raw)); err != nil {
			return err
		}
		refs := collectRefs(st, id, view)
		for _, ref := range refs {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO view_refs(component_id, ref_kind, ref_key, version)
				VALUES(?,?,?,?)
				ON CONFLICT(component_id, ref_kind, ref_key)
				DO UPDATE SET version=excluded.version`,
				id, ref.kind, ref.key, version); err != nil {
				return err
			}
		}
	}

	affected := computeAffected(st, triggerKeys, false)
	setJSON, err := json.Marshal(affected)
	if err != nil {
		return err
	}
	keysJSON, _ := json.Marshal(triggerKeys)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO affected_sets(version, trigger_keys, unconverged, set_json)
		VALUES(?,?,0,?)
		ON CONFLICT(version) DO UPDATE SET
		  trigger_keys=excluded.trigger_keys, unconverged=excluded.unconverged,
		  set_json=excluded.set_json`,
		version, string(keysJSON), string(setJSON)); err != nil {
		return err
	}
	return nil
}

type viewRef struct {
	kind string // key | prefix | dep_edge
	key  string
}

// collectRefs records every key path, subscription prefix and dependency edge
// referenced by a component's derived view, enabling cascade invalidation.
func collectRefs(st *materializedState, componentID string, view *ComponentView) []viewRef {
	seen := map[viewRef]bool{}
	var refs []viewRef
	add := func(kind, key string) {
		r := viewRef{kind, key}
		if !seen[r] {
			seen[r] = true
			refs = append(refs, r)
		}
	}
	for _, entry := range view.Entries {
		add("key", entry.KeyPath)
		add("prefix", entry.Source.ComponentID+":"+entry.Source.Prefix)
	}
	c := st.components[componentID]
	for _, p := range c.SubPrefixes {
		add("prefix", componentID+":"+p)
	}
	if c.Wildcard {
		add("prefix", componentID+":*")
	}
	for _, up := range c.DependsOn {
		add("dep_edge", componentID+"->"+up)
	}
	return refs
}

// impactedComponents returns every component whose derived structures depend
// on a changed key, prefix or dependency edge: direct subscribers plus the
// whole downstream closure, computed purely from committed facts.
func impactedComponents(st *materializedState, triggerKeys []string) map[string]bool {
	affected := computeAffected(st, triggerKeys, false)
	out := make(map[string]bool, len(affected.Nodes))
	for _, n := range affected.Nodes {
		out[n.ComponentID] = true
	}
	// A full snapshot rebuild touches every component.
	if len(out) == 0 {
		for id := range st.components {
			out[id] = true
		}
	}
	return out
}
