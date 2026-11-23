package pipeline

import (
	"database/sql"

	"mitmpin/internal/sample"
)

// InjectSnapshotGap 直接在 SQLite 层制造检查点版本链跳号：
// 把目标连接最后一个检查点的 checkpoint_version 加 2。
// 仅用于异常注入验证，不修改任何源码；重启恢复时应得到 E_SNAPSHOT_VERSION。
func (p *Pipeline) InjectSnapshotGap(connID string) error {
	db := p.Store().DB()
	var maxVer int
	row := db.QueryRow(
		`SELECT MAX(checkpoint_version) FROM checkpoints WHERE conn_id = ?`, connID)
	if err := row.Scan(&maxVer); err != nil {
		if err == sql.ErrNoRows {
			return sample.NewBizError(sample.E_SNAPSHOT_VERSION, "连接无检查点，无法注入跳号")
		}
		return err
	}
	_, err := db.Exec(
		`UPDATE checkpoints SET checkpoint_version = ? WHERE conn_id = ? AND checkpoint_version = ?`,
		maxVer+2, connID, maxVer)
	return err
}

// Recover 重放单条连接以验证中断恢复，复用引擎检查点链。
func (p *Pipeline) Recover(connID string, bound sample.Snapshot, maxEvents int) (any, error) {
	obs, err := p.loadObservation(connID)
	if err != nil {
		return nil, err
	}
	return p.eng.Process(obs, bound, maxEvents)
}

func (p *Pipeline) loadObservation(connID string) (sample.Observation, error) {
	db := p.Store().DB()
	var payload string
	row := db.QueryRow(`SELECT payload FROM observations WHERE conn_id = ?`, connID)
	if err := row.Scan(&payload); err != nil {
		return sample.Observation{}, err
	}
	var obs sample.Observation
	jsonUnmarshal([]byte(payload), &obs)
	return obs, nil
}
