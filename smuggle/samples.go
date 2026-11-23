package smuggle

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// sampleSpecs 是本地确定性样本集：连接标识 -> (代理视角, 后端视角)。
var sampleSpecs = map[string][2]string{
	"normal/consistent": {
		"GET /api/v1/items HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 5\r\nX-Trace: a1\r\n\r\nhello",
		// 仅头部顺序与大小写不同，边界必须一致，不得误报。
		"GET /api/v1/items HTTP/1.1\r\nx-trace: a1\r\nHOST: example.internal\r\ncontent-LENGTH: 5\r\n\r\nhello",
	},
	"count/mismatch": {
		"POST /submit HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 5\r\n\r\nhello",
		"POST /submit HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 5\r\n\r\nhelloGET /smuggled HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 0\r\n\r\n",
	},
	"boundary/shift": {
		"POST /upload HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 5\r\n\r\nhello",
		"POST /upload HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 8\r\n\r\nhelloXYZ",
	},
	"ambiguity/clte": {
		"POST /submit HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
		"POST /submit HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
	},
	"inject/cl-te": {
		"POST /pay HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
		"POST /pay HTTP/1.1\r\nHost: example.internal\r\nTransfer-Encoding: chunked\r\nContent-Length: 4\r\n\r\n0\r\n\r\n",
	},
	"inject/cl-conflict": {
		"POST /dup HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 5\r\nContent-Length: 9\r\n\r\nhello",
		"POST /dup HTTP/1.1\r\nHost: example.internal\r\nContent-Length: 9\r\nContent-Length: 5\r\n\r\nhello",
	},
	"inject/chunk-leading-zero": {
		"POST /chunk HTTP/1.1\r\nHost: example.internal\r\nTransfer-Encoding: chunked\r\n\r\n000A\r\n0123456789\r\n0\r\n\r\n",
		"POST /chunk HTTP/1.1\r\nHost: example.internal\r\nTransfer-Encoding: chunked\r\n\r\n000A\r\n0123456789\r\n0\r\n\r\n",
	},
	"inject/obs-fold-chunk": {
		"POST /fold HTTP/1.1\r\nHost: example.internal\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n 5\r\nworld\r\n0\r\n\r\n",
		"POST /fold HTTP/1.1\r\nHost: example.internal\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n 5\r\nworld\r\n0\r\n\r\n",
	},
	"inject/unknown-te": {
		"POST /te HTTP/1.1\r\nHost: example.internal\r\nTransfer-Encoding: gzip\r\n\r\n",
		"POST /te HTTP/1.1\r\nHost: example.internal\r\nTransfer-Encoding: gzip\r\n\r\n",
	},
}

// WriteSamples 将确定性样本集写入目录（程序内生成，不依赖网络）。
func WriteSamples(dir string) error {
	for id, pair := range sampleSpecs {
		for suffix, body := range map[string]string{".proxy.raw": pair[0], ".backend.raw": pair[1]} {
			p := filepath.Join(dir, id+suffix)
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadSamples 从目录加载成对样本，按连接相对路径稳定排序。
func LoadSamples(dir string) ([]Connection, error) {
	var conns []Connection
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".proxy.raw") {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		id := strings.TrimSuffix(filepath.ToSlash(rel), ".proxy.raw")
		proxy, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		backend, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(id+".backend.raw")))
		if err != nil {
			return err
		}
		conns = append(conns, Connection{ID: id, Proxy: proxy, Backend: backend})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(conns, func(i, j int) bool { return conns[i].ID < conns[j].ID })
	return conns, nil
}
