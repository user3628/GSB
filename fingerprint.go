package prefixcache

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

func digestParts(domain string, parts ...string) string {
	h := sha256.New()
	writeHashedPart(h, domain)
	for _, part := range parts {
		writeHashedPart(h, part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

type byteWriter interface {
	Write([]byte) (int, error)
}

func writeHashedPart(w byteWriter, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = w.Write(length[:])
	_, _ = w.Write([]byte(value))
}

func blockFingerprint(sourceIndex, chunkIndex int, msg Message, role, language, text string) string {
	return digestParts(
		"prefixcache-block-v1",
		blockFormatVersion,
		strconv.Itoa(sourceIndex),
		strconv.Itoa(msg.Turn),
		role,
		language,
		strconv.Itoa(chunkIndex),
		text,
	)
}

func prefixKey(fingerprints []string) string {
	parts := append([]string{"prefixcache-prefix-v1"}, fingerprints...)
	return digestParts(parts[0], parts[1:]...)
}

func planDigest(plan RequestPlan) string {
	plan.PlanDigest = ""
	data, _ := json.Marshal(plan)
	return digestParts("prefixcache-plan-v1", string(data))
}

func slotDigest(slots []Slot) string {
	ordered := append([]Slot(nil), slots...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].SlotNo < ordered[j].SlotNo })
	var b strings.Builder
	for _, slot := range ordered {
		owners := append([]string(nil), slot.Owners...)
		sort.Strings(owners)
		b.WriteString(strconv.Itoa(slot.SlotNo))
		b.WriteByte('|')
		b.WriteString(slot.PrefixKey)
		b.WriteByte('|')
		b.WriteString(strconv.Itoa(slot.BlockCount))
		b.WriteByte('|')
		b.WriteString(strconv.Itoa(slot.PrefixChars))
		b.WriteByte('|')
		b.WriteString(strconv.FormatInt(slot.OccupiedAt, 10))
		b.WriteByte('|')
		b.WriteString(strings.Join(owners, ","))
		b.WriteByte('\n')
	}
	return digestParts("prefixcache-slot-digest-v1", b.String())
}

func slotForPrefix(key string, slotCount int) int {
	if slotCount <= 0 {
		return 0
	}
	sum := sha256.Sum256([]byte("prefixcache-slot-v1|" + key))
	return int(binary.BigEndian.Uint64(sum[:8]) % uint64(slotCount))
}
