package verify

import (
	"path/filepath"
	"testing"
)

func TestFullAcceptanceVerification(t *testing.T) {
	out := filepath.Join("..", "..", "artifacts", "test-verification")
	rep, err := Run(out)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		for _, ds := range rep.Datasets {
			for _, c := range ds.Checks {
				if !c.OK {
					t.Errorf("%s/%s: %s", ds.Key, c.Name, c.Detail)
				}
			}
		}
		t.Fatal("acceptance verification failed")
	}
}
