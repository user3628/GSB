// 本地 HTTP 测试服务：模拟模型后端，提供确定性延迟与结果摘要。
// 端口从环境变量 BACKEND_PORT 读取；BACKEND_MODE=ok|refuse|timeout 模拟故障。
package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	mode := os.Getenv("BACKEND_MODE")
	if mode == "" {
		mode = "ok"
	}
	port := os.Getenv("BACKEND_PORT")
	if port == "" {
		port = "9001"
	}
	name := os.Getenv("BACKEND_NAME")
	if name == "" {
		name = "backend-" + port
	}
	if mode == "refuse" {
		// 模拟连接拒绝：直接退出，端口无人监听
		log.Printf("backend %s 以 refuse 模式退出（模拟连接拒绝）", name)
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/infer", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if mode == "timeout" {
			time.Sleep(30 * time.Second) // 模拟超时
		}
		// 确定性延迟与摘要：仅取决于请求内容
		latency := int64(5 + len(body)%7)
		time.Sleep(time.Duration(latency) * time.Millisecond)
		h := fnv.New64a()
		h.Write(body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"digest":"%016x","latency_ms":%d,"backend":%q}`,
			h.Sum64(), latency, name)
	})
	log.Printf("backend %s 监听 127.0.0.1:%s 模式=%s", name, port, mode)
	log.Fatal(http.ListenAndServe("127.0.0.1:"+port, mux))
}
