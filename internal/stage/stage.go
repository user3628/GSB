package stage

const (
	Tokenize    = "tokenize"
	Stats       = "stats"
	Postings    = "postings"
	Diagnostics = "diagnostics"
)

var Order = []string{Tokenize, Stats, Postings, Diagnostics}

func Index(name string) int {
	for i, candidate := range Order {
		if candidate == name {
			return i
		}
	}
	return -1
}
