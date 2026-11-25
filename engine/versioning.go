package engine

import "fmt"

type VersionInfo struct {
	Sequence      int64  `json:"sequence"`
	Generation    int64  `json:"generation"`
	DatasetDigest string `json:"dataset_digest"`
	ConfigDigest  string `json:"config_digest"`
}

type VersionRequest struct {
	Sequence      int64  `json:"sequence"`
	Generation    int64  `json:"generation,omitempty"`
	DatasetDigest string `json:"dataset_digest"`
	ConfigDigest  string `json:"config_digest"`
	Replacement   bool   `json:"replacement,omitempty"`
}

type VersionDecision struct {
	Action            string `json:"action"`
	CurrentSequence   int64  `json:"current_sequence"`
	RequestedSequence int64  `json:"requested_sequence"`
	Generation        int64  `json:"generation"`
	Rollback          bool   `json:"rollback"`
}

const (
	ActionNormalAdvance     = "normal_advance"
	ActionFullReplacement   = "full_replacement"
	ActionDuplicateRejected = "duplicate_rejected"
	ActionRollbackRejected  = "rollback_rejected"
)

func ClassifyVersion(cur VersionInfo, req VersionRequest) (VersionDecision, error) {
	requested := req.Sequence
	if requested == 0 {
		requested = cur.Sequence + 1
	}
	gen := cur.Generation
	if req.Replacement || req.Generation == cur.Generation+1 || (req.ConfigDigest != "" && cur.ConfigDigest != "" && req.ConfigDigest != cur.ConfigDigest) {
		gen = cur.Generation + 1
	}
	dec := VersionDecision{CurrentSequence: cur.Sequence, RequestedSequence: requested, Generation: gen}

	if req.Generation > 0 && req.Generation < cur.Generation {
		dec.Action = ActionRollbackRejected
		dec.Rollback = true
		return dec, &Error{Code: CodeStaleCheckpoint, ExpectedVersion: cur.Sequence + 1, ActualVersion: requested,
			Message: fmt.Sprintf("generation rollback %d -> %d rejected", cur.Generation, req.Generation)}
	}
	if requested < cur.Sequence {
		dec.Action = ActionRollbackRejected
		dec.Rollback = true
		return dec, &Error{Code: CodeStaleCheckpoint, ExpectedVersion: cur.Sequence + 1, ActualVersion: requested,
			Message: fmt.Sprintf("version rollback %d -> %d rejected", cur.Sequence, requested)}
	}
	if requested == cur.Sequence {
		sameContent := req.DatasetDigest == cur.DatasetDigest && (req.ConfigDigest == "" || req.ConfigDigest == cur.ConfigDigest) && !req.Replacement
		if sameContent {
			dec.Action = ActionDuplicateRejected
			return dec, &Error{Code: CodeStaleCheckpoint, ExpectedVersion: cur.Sequence + 1, ActualVersion: requested, Message: "duplicate version rejected"}
		}
		dec.Action = ActionRollbackRejected
		dec.Rollback = true
		return dec, &Error{Code: CodeStaleCheckpoint, ExpectedVersion: cur.Sequence + 1, ActualVersion: requested, Message: "content replacement at non-advanced sequence rejected"}
	}
	if requested != cur.Sequence+1 {
		return dec, &Error{Code: CodeStaleCheckpoint, ExpectedVersion: cur.Sequence + 1, ActualVersion: requested, Message: "version gap rejected"}
	}

	configChanged := req.ConfigDigest != "" && cur.ConfigDigest != "" && req.ConfigDigest != cur.ConfigDigest
	if req.Replacement || configChanged || req.Generation == cur.Generation+1 {
		dec.Action = ActionFullReplacement
		return dec, nil
	}
	dec.Action = ActionNormalAdvance
	return dec, nil
}

func (e *Engine) RegisterVersion(req VersionRequest) (*VersionDecision, error) {
	var dec *VersionDecision
	err := e.withLock(func() error {
		cp, err := e.LoadCheckpoint()
		if err != nil {
			return err
		}
		cur := VersionInfo{Sequence: cp.EventSequence, Generation: cp.Generation, DatasetDigest: cp.DatasetDigest, ConfigDigest: cp.ConfigDigest}
		d, err := ClassifyVersion(cur, req)
		if err != nil {
			return err
		}
		dec = &d
		tx, err := e.db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if dec.Action == ActionFullReplacement {
			if _, err := tx.Exec("UPDATE inference_cache SET valid=0"); err != nil {
				return err
			}
			if _, err := tx.Exec("UPDATE unit_versions SET valid=0,is_current=0"); err != nil {
				return err
			}
			if _, err := tx.Exec("UPDATE units SET current_version=0,state_digest=''"); err != nil {
				return err
			}
			cp.Generation = dec.Generation
			if req.ConfigDigest != "" && req.ConfigDigest != cp.ConfigDigest {
				// Config replacement is accepted as a new generation; callers reopen/rebuild with the new Config.
				if err := setMetaTx(tx, "pending_config_digest", req.ConfigDigest); err != nil {
					return err
				}
			}
			if err := saveCheckpointTx(tx, cp); err != nil {
				return err
			}
			if err := setMetaTx(tx, "generation", fmt.Sprint(dec.Generation)); err != nil {
				return err
			}
		}
		if err := insertVersionEventTx(tx, dec.RequestedSequence, dec.Action, dec.Generation, req.DatasetDigest, req.ConfigDigest, dec); err != nil {
			return err
		}
		return tx.Commit()
	})
	return dec, err
}

// FullRebuildAndPublish reconstructs every unit from committed samples and
// publishes a new contiguous model version. It is used after whole replacement.
// An optional Config argument performs an atomic config replacement as well.
func (e *Engine) FullRebuildAndPublish(reason string, replacementConfig ...Config) (*Checkpoint, error) {
	if reason == "" {
		reason = "full_rebuild"
	}
	var published *Checkpoint
	err := e.withLock(func() error {
		cfg := e.cfg
		if len(replacementConfig) > 0 {
			cfg = replacementConfig[0].Normalize()
			if err := cfg.Validate(); err != nil {
				return err
			}
		}
		cp, err := e.LoadCheckpoint()
		if err != nil {
			return err
		}
		oldVersion := cp.ModelVersion
		newVersion := oldVersion + 1
		generation := cp.Generation
		samples, err := e.ListSamples()
		if err != nil {
			return err
		}
		datasetDigest := DatasetDigest(samples)
		full, err := RebuildAll(samples, cfg, datasetDigest, newVersion, generation)
		if err != nil {
			return err
		}
		for i := range full.Units {
			full.Units[i].UnitVersion = newVersion
		}
		if err := full.RefreshFingerprint(); err != nil {
			return err
		}

		tx, err := e.db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		res, err := tx.Exec(`UPDATE meta SET value=? WHERE key='current_model_version' AND value=?`,
			fmt.Sprint(newVersion), fmt.Sprint(oldVersion))
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return &Error{Code: CodeStaleCheckpoint, ExpectedVersion: oldVersion + 1, ActualVersion: oldVersion, Message: "conditional full rebuild publication failed"}
		}
		if _, err := tx.Exec("UPDATE unit_versions SET is_current=0 WHERE is_current=1"); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE inference_cache SET valid=0"); err != nil {
			return err
		}
		unitMap := make([]map[string]any, 0, len(full.Units))
		for _, unit := range full.Units {
			stateJSON, err := CanonicalJSON(unit)
			if err != nil {
				return err
			}
			_, err = tx.Exec(`INSERT INTO unit_versions
				(unit_id,version,model_version,sample_seq,batch_id,state_json,update_json,state_digest,valid,is_current)
				VALUES(?,?,?,0,? ,?,'{}',?,1,1)
				ON CONFLICT(unit_id,version) DO UPDATE SET valid=1,is_current=1,state_json=excluded.state_json`,
				unit.UnitID, unit.UnitVersion, newVersion, reason, string(stateJSON), unit.StateDigest)
			if err != nil {
				return staleOrInternal(err, unit.UnitID)
			}
			if _, err := tx.Exec(`INSERT INTO units(unit_id,current_version,state_digest) VALUES(?,?,?)
				ON CONFLICT(unit_id) DO UPDATE SET current_version=excluded.current_version,state_digest=excluded.state_digest`,
				unit.UnitID, unit.UnitVersion, unit.StateDigest); err != nil {
				return err
			}
			unitMap = append(unitMap, map[string]any{"unit_id": unit.UnitID, "version": unit.UnitVersion, "digest": unit.StateDigest})
		}
		unitMapJSON, _ := CanonicalJSON(unitMap)
		_, err = tx.Exec(`INSERT INTO model_versions
			(version,generation,event_sequence,batch_id,sample_id,kind,fingerprint,unit_map_json,dataset_digest,config_digest,parent_version)
			VALUES(?,?,?,?,'',? ,?,?,?,?,?)`,
			newVersion, full.Generation, newVersion, reason, reason, full.Fingerprint, string(unitMapJSON), datasetDigest, full.ConfigDigest, oldVersion)
		if err != nil {
			return err
		}
		if err := saveCheckpointTx(tx, full); err != nil {
			return err
		}
		if len(replacementConfig) > 0 {
			configJSON, err := CanonicalJSON(cfg)
			if err != nil {
				return err
			}
			if err := setMetaTx(tx, "config_json", string(configJSON)); err != nil {
				return err
			}
			if err := setMetaTx(tx, "config_digest", full.ConfigDigest); err != nil {
				return err
			}
			e.cfg = cfg
		}
		if err := setMetaTx(tx, "dataset_digest", datasetDigest); err != nil {
			return err
		}
		if err := setMetaTx(tx, "current_fingerprint", full.Fingerprint); err != nil {
			return err
		}
		if err := setMetaTx(tx, "event_sequence", fmt.Sprint(newVersion)); err != nil {
			return err
		}
		if err := insertVersionEventTx(tx, newVersion, reason, full.Generation, datasetDigest, full.ConfigDigest, map[string]any{"all_units": true}); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		published = full
		return nil
	})
	return published, err
}

// ReplaceConfig classifies a whole-config replacement, invalidates derivations,
// and rebuilds/publishes all units with the new fixed-hash configuration.
func (e *Engine) ReplaceConfig(cfg Config) (*VersionDecision, *Checkpoint, error) {
	cfg = cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}
	cp, err := e.LoadCheckpoint()
	if err != nil {
		return nil, nil, err
	}
	newDigest, err := cfg.Digest()
	if err != nil {
		return nil, nil, err
	}
	decision, err := e.RegisterVersion(VersionRequest{
		Sequence:     cp.EventSequence + 1,
		Generation:   cp.Generation + 1,
		ConfigDigest: newDigest,
		Replacement:  true,
	})
	if err != nil {
		return decision, nil, err
	}
	rebuilt, err := e.FullRebuildAndPublish("config_replacement", cfg)
	return decision, rebuilt, err
}
