基于 Go 实现的 FermaLoop 项目，一款工业发酵罐联控服务，协调培养、补料、尾气、收获与原位清洗运行。

服务通过 JSON API 管理多台发酵罐的批次阶段、配方执行、探头输入、补料背压、公用工程租约和设备联锁。运行状态写入本地事件日志与原子快照，不依赖外部数据库。

使用 Go 1.26.2 构建：

```text
go build -mod=vendor ./...
```

启动服务：

```text
go run ./cmd/fermaloop -addr 127.0.0.1:21249 -data ./var/fermaloop
```

健康检查位于 `/healthz`，业务查询入口包括 `/api/operations`、`/api/equipment`、`/api/interlocks`、`/api/incidents` 和 `/api/batches`。
