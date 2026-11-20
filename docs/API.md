# HTTP API 示例

服务地址固定为回环地址，例如 `http://127.0.0.1:8765`。以下示例使用 `curl`；验证脚本使用 httpx 完成同等调用。

## 创建队列

```bash
curl -s -X POST http://127.0.0.1:8765/v1/queues \
  -H 'content-type: application/json' \
  -d '{"name":"orders","semantics":"at_least_once","visibility_timeout_ms":500}'
```

## 入队

```bash
curl -s -X POST http://127.0.0.1:8765/v1/queues/orders/messages \
  -H 'content-type: application/json' \
  -d '{
    "message_id":"order-1001",
    "body":"created:1001",
    "ttl_ms":60000,
    "produced_at_ms":4102444800000,
    "extensions":{"trace":"local-1001"}
  }'
```

## 领取

```bash
curl -s -X POST http://127.0.0.1:8765/v1/queues/orders/claim \
  -H 'content-type: application/json' \
  -d '{"consumer_id":"worker-1","visibility_timeout_ms":500}'
```

返回的 `message.lease_token` 用于确认。没有可投递消息时 `message` 为 `null`。

## 确认

单条确认：

```bash
curl -s -X POST 'http://127.0.0.1:8765/v1/queues/orders/ack?consumer_id=worker-1' \
  -H 'content-type: application/json' \
  -d '{"message_id":"order-1001","lease_token":"<token>"}'
```

批量确认：

```bash
curl -s -X POST http://127.0.0.1:8765/v1/queues/orders/acks \
  -H 'content-type: application/json' \
  -d '{
    "consumer_id":"worker-1",
    "acks":[
      {"message_id":"order-1001","lease_token":"<token1>"},
      {"message_id":"order-1002","lease_token":"<token2>"}
    ]
  }'
```

批量响应始终给出逐条结果。`succeeded`、`failed`、`partial_success` 是汇总字段。

## 导出

```bash
curl -s 'http://127.0.0.1:8765/v1/export/compact?queue=orders' \
  -o artifacts/orders.bin
curl -s 'http://127.0.0.1:8765/v1/export/jsonl?queue=orders' \
  -o artifacts/orders.jsonl
```

省略 `queue` 则导出全部队列。响应头 `X-MSMQ-Format` 与 `X-MSMQ-Format-Version` 标识格式。

## 导入

```bash
curl -s -X POST 'http://127.0.0.1:8765/v1/import/compact?sub_ms_policy=truncate' \
  -H 'content-type: application/octet-stream' \
  --data-binary @artifacts/orders.bin

curl -s -X POST 'http://127.0.0.1:8765/v1/import/jsonl?sub_ms_policy=truncate' \
  -H 'content-type: application/x-ndjson' \
  --data-binary @artifacts/orders.jsonl
```

成功响应中的 `import_report` 包含格式版本、兼容结论、填充项、时间归一化、冲突集合与插入/覆盖/跳过/拒绝计数。

## 快照与版本

```bash
curl -s http://127.0.0.1:8765/v1/queues/orders/snapshot
curl -s 'http://127.0.0.1:8765/v1/queues/orders/versions?limit=20'
```

快照在同一读事务内返回消息、有效状态计数、确认数、投递记录数和最新版本快照摘要。

## 恢复报告

```bash
curl -s http://127.0.0.1:8765/v1/recovery/report
```

服务重启时，lifespan 在开始接收请求前完成 pending 意图扫描与报告持久化。

## 强杀测试请求头

仅当服务以 `--enable-failpoints` 启动时，请求头才可触发测试用强杀：

```text
X-MSMQ-Failpoint: claim_mid_transaction
X-MSMQ-Failpoint: ack_mid_transaction
X-MSMQ-Failpoint: claim_after_intent
X-MSMQ-Failpoint: import_mid_transaction
```

命中后进程直接 `os._exit(97)`。这不是生产功能，正常启动默认关闭。

