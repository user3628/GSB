package engine

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"queryrewrite/internal/spec"
)

// Cache is the deterministic prefix-slot table. Durable slots are derived from
// accepted scheduled turns and persisted to SQLite; tentative slots exist only
// for intra-run hit detection and are discarded unless promoted in stage 4.
type Cache struct {
	capacity int
	slots    map[string]*spec.Slot // keyed by full fingerprint
	order    []string              // deterministic slot creation order (ids)
}

// NewCache creates an empty cache with a fixed capacity.
func NewCache(capacity int) *Cache {
	if capacity < 0 {
		capacity = 0
	}
	return &Cache{capacity: capacity, slots: map[string]*spec.Slot{}}
}

// Size reports the number of live slots (durable + tentative).
func (c *Cache) Size() int { return len(c.slots) }

// durableSize counts the capacity-constrained durable slots.
func (c *Cache) durableSize() int { return len(c.slots) }

// blockFingerprint hashes one prefix block with the fixed FNV-1a 64-bit hash.
func blockFingerprint(block string, blockIndex int) string {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d|%s", blockIndex, block)
	return fmt.Sprintf("fnv64:%016x", h.Sum64())
}

// fullFingerprint combines the ordered block fingerprints into a slot key.
func fullFingerprint(blocks []spec.PrefixBlock) string {
	parts := make([]string, len(blocks))
	for i, b := range blocks {
		parts[i] = b.Fingerprint
	}
	return strings.Join(parts, "/")
}

// buildBlocks cuts the candidate into cacheable prefix blocks of blockSize
// runes and records each block index, rune span and fingerprint.
func buildBlocks(text string, minPrefix, blockSize int) (int, []spec.PrefixBlock) {
	runes := []rune(text)
	limit := len(runes)
	if limit > minPrefix {
		limit = minPrefix
	}
	blocks := make([]spec.PrefixBlock, 0)
	start := 0
	idx := 0
	for start < limit {
		end := start + blockSize
		if end > limit {
			end = limit
		}
		blocks = append(blocks, spec.PrefixBlock{
			Index:       idx,
			Start:       start,
			End:         end,
			Fingerprint: blockFingerprint(string(runes[start:end]), idx),
		})
		start = end
		idx++
	}
	return limit, blocks
}

// Stage2 evaluates every candidate against the cache in candidate order. It
// records exact/partial/non-reusable outcomes plus contention and eviction
// evidence, and inserts tentative slots so later candidates see real hits.
func Stage2(cands []spec.Candidate, p spec.Policy, cache *Cache, turn int) []spec.HitResult {
	results := make([]spec.HitResult, 0, len(cands))
	claimed := map[string]string{} // full fingerprint -> first candidate this turn
	for _, cand := range cands {
		hr := spec.HitResult{CandidateID: cand.ID}
		prefixLen, blocks := buildBlocks(cand.Text, p.MinPrefixRunes, p.PrefixBlockRunes)
		hr.PrefixRunes = prefixLen
		hr.Blocks = blocks

		if prefixLen < p.MinPrefixRunes {
			hr.Cacheable = false
			hr.Outcome = spec.PrefixBelow
			hr.Reason = fmt.Sprintf("prefix runes %d < min_cacheable %d", prefixLen, p.MinPrefixRunes)
			results = append(results, hr)
			continue
		}
		hr.Cacheable = true
		fp := fullFingerprint(blocks)

		if slot, ok := cache.slots[fp]; ok {
			hr.Outcome = spec.HitExact
			hr.MatchedSlot = slot.SlotID
			hr.MatchedBlocks = len(blocks)
			hr.Reason = fmt.Sprintf("all %d prefix blocks match slot %s", len(blocks), slot.SlotID)
			results = append(results, hr)
			continue
		}

		bestSlot, bestN := "", 0
		for _, sid := range cache.order {
			slot := cache.slotByID(sid)
			if slot == nil {
				continue
			}
			n := sharedPrefixBlocks(slot, blocks)
			if n > bestN {
				bestN, bestSlot = n, slot.SlotID
			}
		}
		if bestN > 0 {
			hr.Outcome = spec.HitPartial
			hr.MatchedSlot = bestSlot
			hr.MatchedBlocks = bestN
			hr.Reason = fmt.Sprintf("first %d of %d blocks reuse slot %s", bestN, len(blocks), bestSlot)
		} else {
			hr.Outcome = spec.HitReusable
			hr.Reason = "no shared prefix block with any live slot"
		}

		if owner, ok := claimed[fp]; ok {
			hr.ContentionWith = append(hr.ContentionWith, owner)
			hr.Reason += fmt.Sprintf("; contention: fingerprint also claimed by %s in same turn", owner)
		} else {
			claimed[fp] = cand.ID
		}

		// Stage 2 is a pure read-only decision: it never mutates the cache. It
		// records what eviction *would* be required if stage 4 accepts this
		// candidate; the actual slot write/eviction happens deterministically in
		// stage 4, which keeps post-crash replay identical.
		if _, live := cache.slots[fp]; !live && cache.durableSize() >= cache.capacity {
			victim := cache.evictionVictim(turn)
			if victim != nil {
				hr.EvictionOf = victim.SlotID
				hr.EvictionReason = fmt.Sprintf(
					"durable cache full (%d/%d); LRU victim slot %s age=%d on accept",
					cache.durableSize(), cache.capacity, victim.SlotID, turn-victim.LastUsedTurn)
			} else {
				hr.EvictionReason = fmt.Sprintf("cache full (%d/%d); no evictable durable slot",
					cache.durableSize(), cache.capacity)
			}
		}
		results = append(results, hr)
	}
	return results
}

func sharedPrefixBlocks(slot *spec.Slot, blocks []spec.PrefixBlock) int {
	parts := strings.Split(slot.Fingerprint, "/")
	n := 0
	for i, b := range blocks {
		if i < len(parts) && parts[i] == b.Fingerprint {
			n++
		} else {
			break
		}
	}
	return n
}

// evictionVictim selects the deterministic LRU durable slot: greatest age,
// ties broken by smallest slot id.
func (c *Cache) evictionVictim(turn int) *spec.Slot {
	var victim *spec.Slot
	ids := append([]string(nil), c.order...)
	sort.Strings(ids)
	for _, sid := range ids {
		sl := c.slotByID(sid)
		if sl == nil {
			continue
		}
		if victim == nil {
			victim = sl
			continue
		}
		ageS := turn - sl.LastUsedTurn
		ageV := turn - victim.LastUsedTurn
		if ageS > ageV || (ageS == ageV && sl.SlotID < victim.SlotID) {
			victim = sl
		}
	}
	return victim
}

// ApplyEviction removes a slot by id.
func (c *Cache) ApplyEviction(slotID string) bool {
	fp, ok := func() (string, bool) {
		for f, sl := range c.slots {
			if sl.SlotID == slotID {
				return f, true
			}
		}
		return "", false
	}()
	if !ok {
		return false
	}
	delete(c.slots, fp)
	for i, sid := range c.order {
		if sid == slotID {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	return true
}

// slotIDFor derives a stable slot id from the fingerprint so continuous runs
// and post-crash replay create identical ids without a monotonic counter.
func slotIDFor(fp string) string {
	h := fnv.New64a()
	h.Write([]byte(fp))
	return fmt.Sprintf("slot-%016x", h.Sum64())
}

// Put inserts a durable slot, promoting an existing tentative one if present.
func (c *Cache) Put(fp string, blockCount int, session string, turn, birth int) *spec.Slot {
	if sl, ok := c.slots[fp]; ok {
		sl.OwnerSession = session
		sl.LastUsedTurn = turn
		return sl
	}
	sl := &spec.Slot{
		SlotID: slotIDFor(fp), Fingerprint: fp, BlockCount: blockCount,
		OwnerSession: session, OwnerTurn: turn, BirthTurn: birth,
		LastUsedTurn: turn,
	}
	c.slots[fp] = sl
	c.order = append(c.order, sl.SlotID)
	return sl
}

func (c *Cache) slotByID(slotID string) *spec.Slot {
	for _, sl := range c.slots {
		if sl.SlotID == slotID {
			return sl
		}
	}
	return nil
}

// Lookup returns a slot by full fingerprint.
func (c *Cache) Lookup(fp string) (*spec.Slot, bool) {
	sl, ok := c.slots[fp]
	return sl, ok
}

// Slots returns slots in deterministic creation order.
func (c *Cache) Slots() []*spec.Slot {
	out := make([]*spec.Slot, 0, len(c.order))
	for _, sid := range c.order {
		if sl := c.slotByID(sid); sl != nil {
			out = append(out, sl)
		}
	}
	return out
}

// DurableSlots returns the persisted slots in creation order.
func (c *Cache) DurableSlots() []*spec.Slot { return c.Slots() }

// Reset clears every slot (used on full policy replacement).
func (c *Cache) Reset() {
	c.slots = map[string]*spec.Slot{}
	c.order = nil
}

// CandidateFingerprint exposes the full fingerprint for scheduling.
func CandidateFingerprint(hr spec.HitResult) string {
	return fullFingerprint(hr.Blocks)
}
