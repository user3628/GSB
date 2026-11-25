package gen

import "sort"

// synonyms is the fixed local synonym table. Keys are normalized lowercase
// terms; values are replacement candidates in stable priority order. This is
// intentionally embedded so runs never depend on network or external data.
var synonyms = map[string][]string{
	"手机":     {"电话", "终端"},
	"价格":     {"价钱", "报价"},
	"便宜":     {"实惠", "划算"},
	"耳机":     {"耳麦", "耳塞"},
	"蓝牙":     {"bluetooth", "无线"},
	"无线":     {"wireless", "蓝牙"},
	"笔记本":    {"电脑", "便携机"},
	"电脑":     {"计算机", "主机"},
	"相机":     {"照相机", "摄像"},
	"鞋":      {"鞋子", "运动鞋"},
	"跑步":     {"跑鞋", "慢跑"},
	"书":      {"书籍", "图书"},
	"咖啡":     {"coffee", "拿铁"},
	"奶茶":     {"奶绿", "茶饮"},
	"机票":     {"航班", "票务"},
	"酒店":     {"宾馆", "旅店"},
	"快递":     {"速递", "物流"},
	"退货":     {"退款", "售后"},
	"优惠":     {"折扣", "促销"},
	"phone":  {"telephone", "mobile"},
	"cheap":  {"affordable", "inexpensive"},
	"laptop": {"notebook", "computer"},
	"coffee": {"latte", "espresso"},
	"shoe":   {"shoes", "sneaker"},
	"book":   {"books", "novel"},
}

// Synonyms returns the stable synonym list for a term.
func Synonyms(term string) []string {
	return synonyms[term]
}

// SynonymKeys returns all head terms in sorted order for deterministic docs.
func SynonymKeys() []string {
	keys := make([]string, 0, len(synonyms))
	for k := range synonyms {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
