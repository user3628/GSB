# 固定样本说明

`scenario.json` 是 `internal/engine/sample_test.go` 直接读取并回放的确定性场景。

每个对象声明：

- `object_key`、`lower_bound`、`ttl_ms`；
- 一个或多个上传会话，按数组顺序创建；
- 每个会话的 `parts` 包含分片序号、原始内容的标准 Base64、SHA-256 和到达毫秒；
- `submit_order` 指定乱序提交顺序；
- 可选 `duplicate_part_number` 触发同摘要幂等重传；
- `cancel` 或 `merge` 指定会话结束方式；
- 合并会话可选声明 `expected_total_size`、`expected_part_count` 和预期对象摘要。

对象摘要算法为按分片序号升序拼接原始内容字节后计算 SHA-256。

回放命令：

```powershell
go test ./internal/engine -run TestFixedSampleScenario -v
```

