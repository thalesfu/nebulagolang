# nebulagolang

基于 `vesoft-inc/nebula-go` 的 Nebula 图数据库 ORM 层。通过 struct tag 反射自动生成 nGQL DDL / DML，管理连接池（最大 300）和批量操作。

## 核心 struct tag

| tag | 说明 |
|-----|------|
| `nebulakey:"vid"` | 节点唯一标识 |
| `nebulakey:"edgefrom"` / `"edgeto"` | 边的起点 / 终点 |
| `nebulaproperty:"xxx"` | 属性名 |
| `nebulaindexes:"xxx"` | 创建索引的属性 |
| `nebulatagname:"xxx"` | Tag（节点类型）名称 |
| `nebulaedgename:"xxx"` | Edge（关系类型）名称 |

## 主要 API

```go
nebulagolang.InsertVertexes(space, v...)
nebulagolang.BatchInsertVertexes(space, batchSize, vs)
nebulagolang.InsertEdges(space, e...)
nebulagolang.GetAllVertexesByQuery[T](space, query)
nebulagolang.CompareAndUpdateNebulEntityBySliceAndQuery[T](space, ns, query, keepDetail)
```

## 配置

模块目录下放 `nebula-account.yaml`（已加入 .gitignore）：

```yaml
host: localhost
port: 9669
username: root
password: nebula
```

详细操作指南见 [docs/NEBULA_GUIDE.md](docs/NEBULA_GUIDE.md)。
