// testupstream 提供本地回环测试端点：
//   - /healthz：返回约定状态码与响应体标记
//   - /upstream：接收调度器真实 POST
//   - /slow：长时间不响应，用于触发真实探测超时
//
// 连接拒绝通过“未监听端口”天然产生，无需进程参与。
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

func main() {
	port := flag.Int("port", 19090, "listen port")
	marker := flag.String("marker", "UPSTREAM-OK", "response body marker")
	status := flag.Int("status", http.StatusOK, "expected status code")
	slowMillis := flag.Int("slow-millis", 5000, "slow endpoint delay")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(*status)
		_, _ = fmt.Fprintf(w, "healthy marker=%s\n", *marker)
	})
	mux.HandleFunc("/upstream", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Client-Id", r.Header.Get("X-Client-Id"))
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, *marker)
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		timer := time.NewTimer(time.Duration(*slowMillis) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintln(w, "slow-done")
		}
	})
	server := &http.Server{
		Addr:              fmt.Sprintf("127.0.0.1:%d", *port),
		Handler:           mux,
		ReadHeaderTimeout: time.Second,
	}
	log.Printf("test upstream listening on 127.0.0.1:%d marker=%s", *port, *marker)
	log.Fatal(server.ListenAndServe())
}
