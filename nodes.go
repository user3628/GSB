package blockpool

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

func (p *Pool) CreateNamespace(ctx context.Context, name string) (Namespace, *Error) {
	if strings.TrimSpace(name) == "" {
		return Namespace{}, Errorf(CodeSpecInvalid, "namespace is required")
	}
	if len(name) > 255 || strings.ContainsRune(name, '\x00') {
		return Namespace{}, Errorf(CodeSpecInvalid, "namespace is invalid", "namespace", name)
	}
	unlock, lockErr := p.locks.Lock(ctx, []string{"ns:" + name})
	if lockErr != nil {
		return Namespace{}, lockErr
	}
	defer unlock()
	tx, err := p.beginWrite(ctx)
	if err != nil {
		return Namespace{}, Errorf(CodeInternal, "begin namespace transaction", "cause", err.Error())
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `INSERT INTO namespaces(name) VALUES (?) ON CONFLICT(name) DO NOTHING`, name); err != nil {
		return Namespace{}, Errorf(CodeInternal, "create namespace", "cause", err.Error())
	}
	if err := tx.Commit(); err != nil {
		return Namespace{}, Errorf(CodeInternal, "commit namespace", "cause", err.Error())
	}
	return Namespace{Name: name}, nil
}

func (p *Pool) CreateNode(ctx context.Context, ref NodeRef) (NodeRef, *Error) {
	if err := validateNode(ref.Namespace, ref.Path); err != nil {
		return NodeRef{}, err
	}
	unlock, lockErr := p.locks.Lock(ctx, []string{nodeKey(ref.Namespace, ref.Path), "ns:" + ref.Namespace})
	if lockErr != nil {
		return NodeRef{}, lockErr
	}
	defer unlock()
	tx, err := p.beginWrite(ctx)
	if err != nil {
		return NodeRef{}, Errorf(CodeInternal, "begin node transaction", "cause", err.Error())
	}
	defer rollback(tx)
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM namespaces WHERE name=?`, ref.Namespace).Scan(&exists); err != nil {
		return NodeRef{}, Errorf(CodeInternal, "check namespace", "cause", err.Error())
	}
	if exists == 0 {
		return NodeRef{}, Errorf(CodeSpecInvalid, "namespace is not declared", "namespace", ref.Namespace)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO nodes(namespace, path, created_at_ms)
		VALUES (?, ?, ?) ON CONFLICT(namespace, path) DO NOTHING`, ref.Namespace, ref.Path, p.nowMS()); err != nil {
		return NodeRef{}, Errorf(CodeInternal, "create node", "cause", err.Error())
	}
	if err := tx.Commit(); err != nil {
		return NodeRef{}, Errorf(CodeInternal, "commit node", "cause", err.Error())
	}
	return ref, nil
}

func namespaceDeclared(ctx context.Context, tx *sql.Tx, namespace string) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM namespaces WHERE name=?`, namespace).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func nodeExists(ctx context.Context, tx *sql.Tx, namespace, path string) (bool, error) {
	var n int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM nodes WHERE namespace=? AND path=?`, namespace, path).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return n > 0, nil
}
