package generator

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"dialogalign/internal/atomicfs"
	"dialogalign/internal/domain"
)

const Seed int64 = 20261125

type seeded struct {
	state uint64
}

func newSeeded(seed int64) *seeded {
	return &seeded{state: uint64(seed)}
}

// next implements a small, fully specified LCG so generated values do not
// depend on the Go runtime's math/rand implementation.
func (r *seeded) next() uint64 {
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}

func (r *seeded) between(min, max int64) int64 {
	if max <= min {
		return min
	}
	return min + int64(r.next()%uint64(max-min+1))
}

func Paths(root string) (sessions, roles, config, active, maps string) {
	data := filepath.Join(root, "data")
	return filepath.Join(data, "sessions.jsonl"),
		filepath.Join(data, "roles.json"),
		filepath.Join(data, "config.json"),
		filepath.Join(data, "active-map.json"),
		filepath.Join(data, "role_map_")
}

func Generate(root string) error {
	rng := newSeeded(Seed)
	catalog, maps, config := fixtures()
	sessions := sessions(rng)

	var jsonl bytes.Buffer
	enc := domainJSONLineWriter{w: &jsonl}
	for _, turn := range sessions {
		if err := enc.write(turn); err != nil {
			return err
		}
	}

	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	if err := atomicfs.WriteFile(filepath.Join(dataDir, "sessions.jsonl"), jsonl.Bytes(), 0o644); err != nil {
		return err
	}
	if err := atomicfs.WriteJSON(filepath.Join(dataDir, "roles.json"), catalog, true); err != nil {
		return err
	}
	if err := atomicfs.WriteJSON(filepath.Join(dataDir, "config.json"), config, true); err != nil {
		return err
	}
	versions := make([]string, 0, len(maps))
	for version := range maps {
		versions = append(versions, version)
	}
	sort.Strings(versions)
	for _, version := range versions {
		path := filepath.Join(dataDir, "role_map_"+version+".json")
		if err := atomicfs.WriteJSON(path, maps[version], true); err != nil {
			return err
		}
	}
	activePath := filepath.Join(dataDir, "active-map.json")
	if _, err := os.Stat(activePath); os.IsNotExist(err) {
		if err := atomicfs.WriteJSON(activePath, domain.ActiveMap{Version: "v1"}, true); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	return RefreshManifest(root)
}

// RefreshManifest keeps the convenience checksum file in sync after imports or
// role-map edits. It deliberately covers only local sample inputs, not the
// SQLite checkpoint database.
func RefreshManifest(root string) error {
	dataDir := filepath.Join(root, "data")
	manifest := map[string]string{}
	names := []string{"sessions.jsonl", "roles.json", "config.json", "active-map.json"}
	mapPaths, err := filepath.Glob(filepath.Join(dataDir, "role_map_*.json"))
	if err != nil {
		return err
	}
	for _, path := range mapPaths {
		names = append(names, filepath.Base(path))
	}
	sort.Strings(names)
	for _, name := range names {
		path := filepath.Join(dataDir, name)
		blob, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		manifest[name] = domain.DigestBytes(blob)
	}
	return atomicfs.WriteJSON(filepath.Join(dataDir, "manifest.json"), manifest, true)
}

type domainJSONLineWriter struct{ w *bytes.Buffer }

func (w *domainJSONLineWriter) write(v any) error {
	b, err := domain.CanonicalJSON(v)
	if err != nil {
		return err
	}
	w.w.Write(b)
	w.w.WriteByte('\n')
	return nil
}

func fixtures() (domain.RoleCatalog, map[string]domain.RoleMap, domain.WorkbenchConfig) {
	roles := []domain.RoleDef{
		role("order.order_id", intRange(1000, 999999), "订单编号"),
		role("order.channel", enumRange("app", "web", "phone"), "下单渠道"),
		role("order.priority", intRange(0, 3), "订单优先级"),
		role("delivery.city", enumRange("shanghai", "beijing", "shenzhen", "guangzhou"), "配送城市"),
		role("delivery.date", dateRange("2026-01-01", "2026-12-31"), "配送日期区间"),
		role("delivery.address", enumRange("home", "office", "pickup"), "交付地址类型"),
		role("account.email", enumRange("primary", "secondary", "shared"), "账户邮箱类型"),
		role("profile.email", enumRange("primary", "secondary", "shared"), "资料邮箱类型"),
		role("billing.amount", intRange(1, 9999), "账单金额"),
		role("account.level", enumRange("gold", "silver", "bronze"), "账户等级"),
		role("subscription.start_date", dateRange("2026-01-01", "2026-12-31"), "订阅开始日期区间"),
		role("refund.reason", enumRange("damaged", "wrong", "late"), "退款原因"),
	}
	catalog := domain.RoleCatalog{Version: "roles-v1", Roles: roles}
	entries := []domain.RoleMapEntry{
		entry(101, "order.order_id", 10, intRange(1, 999999999), intRange(1000, 999999), "订单号旧码 A"),
		entry(102, "order.order_id", 10, intRange(1, 999999999), intRange(1000, 999999), "订单号旧码 B"),
		entry(201, "delivery.city", 10, enumRange("shanghai", "beijing", "shenzhen", "guangzhou"), enumRange("shanghai", "beijing", "shenzhen", "guangzhou"), ""),
		entry(202, "delivery.city", 10, enumRange("shanghai", "beijing", "shenzhen", "guangzhou"), enumRange("shanghai", "beijing", "shenzhen", "guangzhou"), ""),
		entry(301, "delivery.date", 20, dateRange("2026-01-01", "2026-12-31"), dateRange("2026-01-01", "2026-12-31"), "v1 date role"),
		entry(302, "subscription.start_date", 20, dateRange("2026-01-01", "2026-12-31"), dateRange("2026-01-01", "2026-12-31"), ""),
		entry(401, "refund.reason", 10, enumRange("damaged", "wrong", "late"), enumRange("damaged", "wrong", "late"), ""),
		entry(501, "account.email", 20, enumRange("primary", "secondary", "shared"), enumRange("primary", "secondary", "shared"), "ambiguous shared preference A"),
		entry(501, "profile.email", 20, enumRange("primary", "secondary", "shared"), enumRange("primary", "secondary", "shared"), "ambiguous shared preference B"),
		entry(502, "billing.amount", 30, intRange(1, 9999), intRange(1, 9999), "numeric target"),
		entry(502, "account.level", 20, enumRange("gold", "silver", "bronze"), enumRange("gold", "silver", "bronze"), "enum target incompatible with numeric raw value"),
		entry(503, "account.email", 30, enumRange("primary", "secondary", "shared"), enumRange("primary", "secondary", "shared"), "priority winner"),
		entry(503, "profile.email", 10, enumRange("primary", "secondary", "shared"), enumRange("primary", "secondary", "shared"), "lower priority"),
		entry(601, "order.channel", 10, enumRange("app", "web", "phone"), enumRange("app", "web", "phone"), ""),
		entry(602, "order.priority", 10, intRange(0, 3), intRange(0, 3), ""),
		entry(603, "delivery.address", 10, enumRange("home", "office", "pickup"), enumRange("home", "office", "pickup"), ""),
		entry(604, "account.level", 10, enumRange("gold", "silver", "bronze"), enumRange("gold", "silver", "bronze"), ""),
		entry(701, "profile.email", 10, enumRange("primary", "secondary", "shared"), enumRange("primary", "secondary", "shared"), "unambiguous profile code"),
	}
	v1 := domain.RoleMap{Version: "v1", CompatibleDeclarations: []string{"v1"}, Entries: entries}
	v2entries := append([]domain.RoleMapEntry(nil), entries...)
	for i := range v2entries {
		if v2entries[i].LegacyCode == 301 {
			v2entries[i].Role = "subscription.start_date"
			v2entries[i].NewRange = dateRange("2026-01-01", "2026-12-31")
			v2entries[i].Note = "v2 intentionally changes the confirmed role"
		}
	}
	v2 := domain.RoleMap{Version: "v2", CompatibleDeclarations: []string{"v1", "v2"}, Entries: v2entries}

	config := domain.WorkbenchConfig{
		Version: "demo-v1",
		Rules: domain.RulesConfig{
			Version:       "rules-v1",
			InitialIntent: "greeting",
			Rules: []domain.IntentRule{
				{ID: "r_order_status", Priority: 100, KeywordsAny: []string{"order", "订单", "status"}, To: "order_status", Clarify: []string{"delivery.date"}},
				{ID: "r_delivery_change", Priority: 90, KeywordsAny: []string{"delivery", "配送", "send", "地址"}, To: "delivery_change", Clarify: []string{"delivery.address"}},
				{ID: "r_account_help", Priority: 80, KeywordsAny: []string{"account", "邮箱", "mailbox", "shared"}, To: "account_help", Clarify: []string{"account.email"}},
				{ID: "r_subscription", Priority: 70, KeywordsAny: []string{"subscription", "订阅"}, To: "subscription_change"},
				{ID: "r_refund", Priority: 60, KeywordsAny: []string{"refund", "退款"}, To: "refund_request", Clarify: []string{"refund.reason"}},
			},
		},
		Normalization: domain.NormalizationConfig{
			Version:          "norm-v1",
			IntegerPrecision: 0,
			EnumCaseFold:     true,
			EnumAliases: map[string]string{
				"上海": "shanghai", "北京": "beijing", "深圳": "shenzhen", "广州": "guangzhou",
				"应用": "app", "网页": "web", "电话": "phone", "主页": "home", "办公室": "office",
			},
			DateLayouts:        []string{"2006-01-02", "2006/01/02", "2006年1月2日"},
			DateRangeSplitters: []string{"..", "~", "至", "到"},
		},
		Alignment: domain.AlignmentConfig{
			Version:                "align-v1",
			OutOfRangeEnabled:      true,
			DriftEnabled:           true,
			AmbiguityEnabled:       true,
			MaxFindingsPerCategory: 100,
		},
	}
	return catalog, map[string]domain.RoleMap{"v1": v1, "v2": v2}, config
}

func sessions(rng *seeded) []domain.TurnInput {
	cityRange := enumRange("shanghai", "beijing", "shenzhen", "guangzhou")
	dateFull := dateRange("2026-01-01", "2026-12-31")
	emailRange := enumRange("primary", "secondary", "shared")
	orderID := 1000 + rng.between(0, 5000)
	orderID2 := 700000 + rng.between(0, 5000)
	amount := rng.between(20, 200)

	turns := []domain.TurnInput{
		newTurn("S01", 1, "你好 hello，帮我查一下 order #"+fmt.Sprint(orderID)+" status，可以吗？", "订单已出库。请告诉我配送城市。", "zh-en", "order_status",
			[]domain.NewSlotAnnotation{newSlot("order.order_id", intRange(1000, 999999), fmt.Sprint(orderID), "order_id", "你好 hello，帮我查一下 order #"+fmt.Sprint(orderID)+" status，可以吗？")}, nil),
		newTurn("S01", 2, "Please send it to 上海，日期 2026年3月1日到2026年3月5日。", "已记录城市和日期，地址类型？", "zh-en", "",
			[]domain.NewSlotAnnotation{newSlot("delivery.city", cityRange, "上海", "delivery_city", "Please send it to 上海，日期 2026年3月1日到2026年3月5日。")},
			[]domain.LegacySlotAnnotation{oldSlot(301, "v1", "2026年3月1日到2026年3月5日", dateFull, "delivery_date", "Please send it to 上海，日期 2026年3月1日到2026年3月5日。")}),
		newTurn("S01", 3, "Use the app channel, priority 9.", "优先级超出可受理范围，请选择 0-3。", "en", "",
			[]domain.NewSlotAnnotation{
				newSlot("order.channel", enumRange("app", "web", "phone"), "app", "channel", "Use the app channel, priority 9."),
				newSlot("order.priority", intRange(0, 3), "9", "priority", "Use the app channel, priority 9."),
			}, nil),
	}

	t1 := "我要设置 shared 联系方式为 primary"
	turns = append(turns, newTurn("S02", 1, t1, "该旧码对应多个语义角色，请澄清。", "zh-en", "account_help", nil,
		[]domain.LegacySlotAnnotation{oldSlot(501, "v1", "primary", emailRange, "contact_preference", t1)}))
	t2 := "账单金额是 " + fmt.Sprint(amount) + " 元"
	turns = append(turns, newTurn("S02", 2, t2, "已记录金额。", "zh", "", nil,
		[]domain.LegacySlotAnnotation{oldSlot(502, "v1", fmt.Sprint(amount), intRange(1, 9999), "amount", t2)}))
	t3 := "Use the shared mailbox, value primary"
	turns = append(turns, newTurn("S02", 3, t3, "已确认账户邮箱。", "en", "", nil,
		[]domain.LegacySlotAnnotation{oldSlot(503, "v1", "primary", emailRange, "account_mailbox", t3)}))
	t4 := "好的谢谢"
	turns = append(turns, newTurn("S02", 4, t4, "不客气。", "zh", "", nil, nil))

	t1 = "I need to move my subscription date to 2026-04-01..2026-04-05"
	turns = append(turns, newTurn("S03", 1, t1, "请确认新日期。", "en", "subscription_change", nil,
		[]domain.LegacySlotAnnotation{oldSlot(301, "v1", "2026-04-01..2026-04-05", dateFull, "subscription_date", t1)}))
	t2 = "订单号 order " + fmt.Sprint(orderID2)
	turns = append(turns, newTurn("S03", 2, t2, "已记录订单号。", "zh-en", "", nil,
		[]domain.LegacySlotAnnotation{oldSlot(101, "v1", fmt.Sprint(orderID2), intRange(1, 999999999), "order_id", t2)}))
	t3 = "Use account level gold"
	turns = append(turns, newTurn("S03", 3, t3, "已记录账户等级。", "en", "", nil,
		[]domain.LegacySlotAnnotation{oldSlot(604, "v1", "gold", enumRange("gold", "silver", "bronze"), "account_level", t3)}))
	t4 = "cancel that"
	turns = append(turns, newTurn("S03", 4, t4, "请告诉我需要取消的对象。", "en", "", nil, nil))

	turns = append(turns, newTurn("S04", 1, "hello", "How can I help?", "en", "greeting", nil, nil))
	turns = append(turns, newTurn("S04", 2, "查一下 order status", "请提供配送日期。", "zh-en", "order_status", nil, nil))
	turns = append(turns, newTurn("S04", 3, "I want delivery change instead", "What address type?", "en", "delivery_change", nil, nil))
	turns = append(turns, newTurn("S04", 4, "back to order status", "需要配送日期。", "en", "order_status", nil, nil))
	turns = append(turns, newTurn("S04", 5, "ok", "还需要日期和地址。", "en", "", nil, nil))
	return turns
}

func newTurn(session string, index int, user, bot, lang, hint string, ns []domain.NewSlotAnnotation, os []domain.LegacySlotAnnotation) domain.TurnInput {
	return domain.TurnInput{SessionID: session, TurnIndex: index, UserText: user, BotReply: bot, Language: lang, IntentHint: hint, NewSlots: ns, OldSlots: os}
}

func newSlot(role string, r domain.ValueRange, value, field, text string) domain.NewSlotAnnotation {
	return domain.NewSlotAnnotation{Role: role, Range: r, Value: value, Field: field, CharOffset: runeOffset(text, value)}
}

func oldSlot(code int, version, raw string, r domain.ValueRange, field, text string) domain.LegacySlotAnnotation {
	return domain.LegacySlotAnnotation{RoleCode: code, MapVersion: version, RawValue: raw, Range: r, Field: field, CharOffset: runeOffset(text, raw)}
}

func runeOffset(text, sub string) int {
	byteOffset := strings.Index(text, sub)
	if byteOffset < 0 {
		return 0
	}
	return utf8.RuneCountInString(text[:byteOffset])
}

func role(name string, r domain.ValueRange, description string) domain.RoleDef {
	return domain.RoleDef{Name: name, Range: r, Description: description}
}

func entry(code int, role string, priority int, legacy, current domain.ValueRange, note string) domain.RoleMapEntry {
	return domain.RoleMapEntry{LegacyCode: code, Role: role, Priority: priority, LegacyRange: legacy, NewRange: current, Note: note}
}

func intRange(min, max int64) domain.ValueRange {
	return domain.ValueRange{Kind: domain.KindInteger, Min: domain.Int64Ptr(min), Max: domain.Int64Ptr(max)}
}

func enumRange(values ...string) domain.ValueRange {
	return domain.ValueRange{Kind: domain.KindEnum, Enum: append([]string(nil), values...)}
}

func dateRange(min, max string) domain.ValueRange {
	return domain.ValueRange{Kind: domain.KindDateRange, DateMin: min, DateMax: max}
}

// LoadTurns parses the generated/imported JSONL fixture.
func LoadTurns(path string) ([]domain.TurnInput, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var turns []domain.TurnInput
	line := 0
	for scanner.Scan() {
		line++
		blob := bytes.TrimSpace(scanner.Bytes())
		if len(blob) == 0 {
			continue
		}
		var turn domain.TurnInput
		if err := jsonUnmarshal(blob, &turn); err != nil {
			return nil, fmt.Errorf("sessions.jsonl line %d: %w", line, err)
		}
		if turn.SessionID == "" || turn.TurnIndex < 1 {
			return nil, fmt.Errorf("sessions.jsonl line %d: session_id and positive turn_index are required", line)
		}
		turns = append(turns, turn)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return turns, nil
}
