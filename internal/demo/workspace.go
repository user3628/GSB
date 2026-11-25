// Package demo bootstraps the deterministic sample workspace.
package demo

import (
	"os"
	"path/filepath"

	"matbench/internal/palette"
	"matbench/internal/recipe"
)

// Layout paths inside a workspace.
const (
	BookPath  = "recipes.json"
	CardsDir  = "cards"
	DBName    = "matbench.db"
	StopFile  = "STOP"
	TexDir    = "artifacts/textures"
	ReportDir = "artifacts/reports"
)

// Setup generates the fixed-seed recipe book and reference cards in dir.
func Setup(dir string) (*recipe.Book, error) {
	if err := os.MkdirAll(filepath.Join(dir, CardsDir), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, TexDir), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, ReportDir), 0o755); err != nil {
		return nil, err
	}
	book := recipe.GenerateBook()
	for _, card := range book.Cards {
		if err := palette.RenderCard(filepath.Join(dir, CardsDir), card,
			book.GeneratorSeed+int64(card.Width)*7+int64(card.Height)*13); err != nil {
			return nil, err
		}
	}
	if err := recipe.WriteBook(book, filepath.Join(dir, BookPath)); err != nil {
		return nil, err
	}
	return book, nil
}

// Load reads the workspace recipe book.
func Load(dir string) (*recipe.Book, error) {
	return recipe.ReadBook(filepath.Join(dir, BookPath))
}

// FileStop implements pipeline.StopChecker via a sentinel file.
type FileStop struct{ Path string }

// StopRequested reports whether the stop sentinel exists.
func (f FileStop) StopRequested() bool {
	_, err := os.Stat(f.Path)
	return err == nil
}

// RequestStop creates the stop sentinel.
func (f FileStop) RequestStop() error {
	if err := os.MkdirAll(filepath.Dir(f.Path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(f.Path, []byte("stop\n"), 0o644)
}

// ClearStop removes the stop sentinel.
func (f FileStop) ClearStop() error {
	if err := os.Remove(f.Path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
