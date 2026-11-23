// 网关主程序：本地推理网关路由与诊断工作台。
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"gsb/internal/api"
	"gsb/internal/router"
	"gsb/internal/store"
)

func main() {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "data"
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dataDir, "gateway.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()

	rtr := router.New(st)
	// 启动即恢复：扫描未完成意图，回滚到最后一个完整版本
	rep, err := rtr.Recover()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("恢复检查: %s", rep.Message)

	srv := api.NewServer(rtr, st)
	port := api.ParsePort("GATEWAY_PORT", 8080)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	log.Printf("网关监听 http://%s （路由表版本 %d）", addr, rtr.Version())
	log.Fatal(http.ListenAndServe(addr, srv.Mux))
}
