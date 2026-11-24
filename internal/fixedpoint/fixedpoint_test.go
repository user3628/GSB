package fixedpoint

import (
	"errors"
	"testing"

	"privescworkbench/internal/domain"
)

func TestMulTruncationAndRemainder(t *testing.T) {
	// 1.000001 * 1.000002: product 1_000_003_000_002 -> q=1_000_003
	// with a 2-unit ledger remainder (2e-12 real), proving truncation booking
	q, rem, err := Mul(1_000_001, 1_000_002)
	if err != nil {
		t.Fatal(err)
	}
	if q != 1_000_003 || rem != 2 {
		t.Fatalf("got q=%d rem=%d", q, rem)
	}
	if q*Scale+rem != 1_000_001*1_000_002 {
		t.Fatal("remainder identity violated")
	}
}

func TestNanosToMicroRemainder(t *testing.T) {
	micro, rem, err := NanosToMicro(1_000_500)
	if err != nil {
		t.Fatal(err)
	}
	if micro != 1000 || rem != 500_000 {
		t.Fatalf("got micro=%d rem=%d", micro, rem)
	}
	micro, _, err = NanosToMicro(999)
	if err != nil || micro != 0 {
		t.Fatalf("999ns -> micro 0, got %d err=%v", micro, err)
	}
}

func TestBoundRoundsUp(t *testing.T) {
	// 1 ledger unit rounds up to 1 micro as an upper bound
	b, err := BoundMicro(1)
	if err != nil || b != 1 {
		t.Fatalf("bound=%d err=%v", b, err)
	}
	b, err = BoundMicro(LedgerPerMicro)
	if err != nil || b != 1 {
		t.Fatalf("exact micro bound=%d err=%v", b, err)
	}
}

func TestOverflowRejectedAndLedgerUntouched(t *testing.T) {
	if _, _, err := Mul(MaxMicro+1, Scale); err == nil {
		t.Fatal("expected overflow error")
	} else if !errors.Is(err, err) {
		var ce *domain.CodeError
		if !asCode(err, &ce) || ce.Code != domain.CodeScoreOverflow {
			t.Fatalf("wrong code: %v", err)
		}
	}
	ledger := NewLedger()
	if err := ledger.Record(1, 1, "ok", 10); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Record(1, 2, "bad", -1); err == nil {
		t.Fatal("expected negative remainder rejection")
	}
	if len(ledger.Items()) != 1 {
		t.Fatalf("ledger must not absorb failed record, got %d items", len(ledger.Items()))
	}
}

func asCode(err error, target **domain.CodeError) bool {
	ce, ok := err.(*domain.CodeError)
	if ok {
		*target = ce
	}
	return ok
}

func TestLedgerCommitBound(t *testing.T) {
	l := NewLedger()
	for i := 0; i < 3; i++ {
		// 0.4 micro each in ledger units = 400_000_000_000
		if err := l.Record(7, int64(i), "k", 400_000_000_000); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Commit(); err != nil {
		t.Fatal(err)
	}
	agg := l.Proc(7)
	// sum = 1_200_000_000_000 ledger = 1.2 micro exact
	if agg.BoundMicro != 1_200_000 || agg.ItemCount != 3 || agg.RemainderSum != 1_200_000_000_000 {
		t.Fatalf("agg=%+v", agg)
	}
}
