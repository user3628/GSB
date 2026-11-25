package pipeline

import "testing"

func validCamera() Camera {
	cam := orbitCamera("view-00", 0.5, 4.0, 16, 16)
	return cam
}

func TestValidateCameraOrthogonality(t *testing.T) {
	cam := validCamera()
	if err := ValidateCamera("shard-00", cam); err != nil {
		t.Fatalf("valid camera rejected: %v", err)
	}
	bad := cam
	bad.R[0] += 0.5 // break orthogonality
	err := ValidateCamera("shard-00", bad)
	if err == nil {
		t.Fatal("non-orthogonal pose accepted")
	}
	be := asBiz(err)
	if be.Code != CodePoseInvalid || be.Field != "r" || be.ShardID != "shard-00" {
		t.Fatalf("unexpected error: %+v", be)
	}
}

func TestValidateCameraZeroFocal(t *testing.T) {
	cam := validCamera()
	cam.K.FX = 0
	err := ValidateCamera("shard-00", cam)
	be := asBiz(err)
	if be.Code != CodePoseInvalid || be.Field != "k.focal" {
		t.Fatalf("unexpected: %+v", be)
	}
}

func TestPixelOutOfRangeAndNaN(t *testing.T) {
	img := &ViewImage{ViewID: "v", Width: 4, Height: 4, Gray: make([]uint8, 16), NaN: make([]bool, 16)}
	if _, err := img.Pixel(4, 0); asBiz(err).Code != CodeOutOfRange {
		t.Fatalf("expected out of range, got %v", err)
	}
	img.NaN[0] = true
	if _, err := img.Pixel(0, 0); asBiz(err).Code != CodeNaNInput {
		t.Fatalf("expected nan input, got %v", err)
	}
}

func TestEmptyViewsAndBadDensity(t *testing.T) {
	m := &Manifest{Config: DefaultConfig()}
	if be := asBiz(ValidateConfigAndManifest(m)); be.Code != CodeEmptyViews {
		t.Fatalf("got %v", be)
	}
	m.Shards = []ShardSpec{{ShardID: "s", Views: []Camera{validCamera()}}}
	m.Config.SampleDensity = 0
	if be := asBiz(ValidateConfigAndManifest(m)); be.Code != CodeBadDensity {
		t.Fatalf("got %v", be)
	}
}

func TestProjectViewProducesContributions(t *testing.T) {
	dir := t.TempDir()
	m, err := GenerateDataset(GenerateParams{OutDir: dir, Seed: 1, Shards: 1, ViewsPer: 1,
		ImageW: 16, ImageH: 16, Config: DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	_, images, err := LoadDataset(dir)
	if err != nil {
		t.Fatal(err)
	}
	cam := m.Shards[0].Views[0]
	recs, err := ProjectView(m.Config, "shard-00", cam, images[cam.ViewID])
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Fatal("expected contributions")
	}
	for _, r := range recs {
		if r.ShardID != "shard-00" || r.ViewID != cam.ViewID {
			t.Fatalf("missing identity on contribution: %+v", r)
		}
	}
}

func TestProjectionDeterministic(t *testing.T) {
	dir := t.TempDir()
	m, _, _ := LoadDataset(mustGen(t, dir))
	_, images, _ := LoadDataset(dir)
	cam := m.Shards[0].Views[0]
	a, _ := ProjectView(m.Config, "shard-00", cam, images[cam.ViewID])
	b, _ := ProjectView(m.Config, "shard-00", cam, images[cam.ViewID])
	da, _ := Digest(sortedContributions(a))
	db, _ := Digest(sortedContributions(b))
	if da != db {
		t.Fatal("projection non-deterministic")
	}
}

func mustGen(t *testing.T, dir string) string {
	t.Helper()
	if _, err := GenerateDataset(GenerateParams{OutDir: dir, Seed: 3, Shards: 2, ViewsPer: 2,
		ImageW: 16, ImageH: 16, Config: DefaultConfig()}); err != nil {
		t.Fatal(err)
	}
	return dir
}
