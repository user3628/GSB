package winpath

import (
	"reflect"
	"testing"
)

func TestAssessNormalizesAndFindsCandidates(t *testing.T) {
	assessment := Assess("C:\\Program Files\\Vendor///Updater.EXE -k run", false)
	if assessment.Malformed {
		t.Fatalf("unexpected malformed path: %s", assessment.MalformedReason)
	}
	if assessment.Normalized != `c:\program files\vendor\updater.exe` {
		t.Fatalf("normalized path = %q", assessment.Normalized)
	}
	if !assessment.Quoted && !assessment.Unquoted {
		t.Fatalf("unquoted path was not detected: %+v", assessment)
	}
	if assessment.Arguments != "-k run" {
		t.Fatalf("arguments = %q", assessment.Arguments)
	}
	wantCandidates := []string{`c:\program.exe`, `c:\program files\vendor\updater.exe`}
	if !reflect.DeepEqual(assessment.ExecutableCandidates, wantCandidates) {
		t.Fatalf("candidates = %#v, want %#v", assessment.ExecutableCandidates, wantCandidates)
	}
}

func TestAssessQuotedEquivalent(t *testing.T) {
	quoted := Assess("\"C:\\Program Files\\Vendor\\updater.exe\" -k run", false)
	unquoted := Assess("c:/program files/vendor//updater.exe -k run", false)
	if quoted.Malformed || unquoted.Malformed {
		t.Fatalf("unexpected malformed: %+v %+v", quoted, unquoted)
	}
	if quoted.ImagePathKey != unquoted.ImagePathKey {
		t.Fatalf("keys differ: %q != %q", quoted.ImagePathKey, unquoted.ImagePathKey)
	}
	if !quoted.Quoted || quoted.Unquoted {
		t.Fatalf("quoted flags are wrong: %+v", quoted)
	}
	if !unquoted.Unquoted {
		t.Fatalf("unquoted flag is wrong: %+v", unquoted)
	}
}

func TestAssessRejectsUnterminatedQuote(t *testing.T) {
	assessment := Assess("\"C:\\Program Files\\Broken\\svc.exe", false)
	if !assessment.Malformed {
		t.Fatalf("expected malformed path")
	}
	if assessment.MalformedReason == "" {
		t.Fatalf("missing malformed reason")
	}
}

func TestAssessWritableDirectory(t *testing.T) {
	assessment := Assess("C:\\ProgramData\\Vendor\\agent.exe --service", true)
	if assessment.Malformed {
		t.Fatalf("unexpected malformed path: %s", assessment.MalformedReason)
	}
	if !assessment.WritableDirectory {
		t.Fatalf("expected writable directory")
	}
	if len(assessment.WritableReasons) != 2 {
		t.Fatalf("writable reasons = %#v", assessment.WritableReasons)
	}
}

func TestAssessKeepsProgramFilesReadOnly(t *testing.T) {
	assessment := Assess("C:\\Program Files\\Vendor\\agent.exe", false)
	if assessment.Malformed {
		t.Fatalf("unexpected malformed path: %s", assessment.MalformedReason)
	}
	if assessment.WritableDirectory {
		t.Fatalf("Program Files should not be marked writable")
	}
}

func TestAssessAllowsBalancedQuotedArguments(t *testing.T) {
	assessment := Assess("C:\\Program Files\\Vendor\\agent.exe --label \"quoted value\"", false)
	if assessment.Malformed {
		t.Fatalf("balanced argument quote was rejected: %s", assessment.MalformedReason)
	}
	if assessment.Normalized != `c:\program files\vendor\agent.exe` || assessment.Arguments != `--label "quoted value"` {
		t.Fatalf("unexpected assessment: %+v", assessment)
	}
}
