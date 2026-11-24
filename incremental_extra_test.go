package cockpitaudio

import "testing"

func TestIncrementalMismatchReturnsFirstInterval(t *testing.T) {
	ds := acceptanceDataset(t)
	cfg := DefaultConfig()
	expected, err := BuildSession(ds, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := BuildSession(ds, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	actual.Shards[0].VAD.Zones[0].Intervals[0].EndMS += 7
	verr := CompareSessionResults(expected, actual)
	ae, ok := AsError(verr)
	if !ok || ae.Code != CodeIncrementalMismatch || ae.FirstDifference == "" {
		t.Fatalf("mismatch error=%#v", verr)
	}
	if got := ae.Diagnostics["first_difference"]; got == "" {
		t.Fatalf("missing first difference diagnostic: %#v", ae.Diagnostics)
	}
}
