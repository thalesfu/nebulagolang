# Nebula 使用指南

本文件记录在本项目中操作 Nebula 图数据库的完整方法，供 Claude 在后续会话中快速准确使用。
**随工作进展持续更新。**

---

## 1. 启动与停止

Nebula 通过 Docker Compose 运行，compose 文件在：

```
/Users/thalesfu/codes/github.com/vesoft-inc/nebula-docker-compose/docker-compose.yaml
```

```bash
# 启动（后台）
cd /Users/thalesfu/codes/github.com/vesoft-inc/nebula-docker-compose
docker compose up -d

# 停止
docker compose down

# 查看容器状态
docker ps | grep nebula
```

启动后等约 5 秒再连接。容器名称：

| 容器 | 用途 |
|------|------|
| `nebula-docker-compose-graphd-1` | Graph 服务（主查询入口） |
| `nebula-docker-compose-storaged0/1/2-1` | 存储节点 |
| `nebula-docker-compose-metad0/1/2-1` | Meta 节点 |
| `nebula-docker-compose-console-1` | 命令行客户端 |

---

## 2. 连接参数

| 参数 | 值 |
|------|---|
| 地址 | `localhost` / `graphd`（容器内） |
| 端口 | `9669` |
| 用户名 | `root` |
| 密码 | `nebula` |
| 版本 | NebulaGraph v3.6.0 |

---

## 3. 执行 nGQL 语句

### 方式一：docker exec（推荐，最常用）

```bash
# 单条语句
docker exec nebula-docker-compose-console-1 nebula-console \
  -addr graphd -port 9669 -u root -p nebula \
  -e "USE ck2; SHOW TAGS;"

# 多条语句（用分号分隔）
docker exec nebula-docker-compose-console-1 nebula-console \
  -addr graphd -port 9669 -u root -p nebula \
  -e "USE ck2; MATCH (n:story) RETURN n LIMIT 5;"

# 换行写法（shell heredoc）
docker exec nebula-docker-compose-console-1 nebula-console \
  -addr graphd -port 9669 -u root -p nebula \
  -e "
USE ck2;
MATCH (n:people) RETURN count(n);
"
```

### 方式二：交互式 console（不推荐，不便于脚本化）

```bash
docker exec -it nebula-docker-compose-console-1 nebula-console \
  -addr graphd -port 9669 -u root -p nebula
```

---

## 4. Space 列表

```nGQL
SHOW SPACES;
-- 输出: ck2 / ffta / howiestudy
```

| Space | 内容 |
|-------|------|
| `ck2` | 《十字军之王 II》完整游戏数据 |
| `ffta` | 《最终幻想战略版 A》数据 |
| `howiestudy` | 其他学习数据 |

---

## 5. ck2 Space 常用操作

### 5.1 基础 DDL 查询

```nGQL
USE ck2;

-- 查看所有 Tag（顶点类型）
SHOW TAGS;

-- 查看所有 Edge 类型
SHOW EDGES;

-- 查看某个 Tag 的 schema
DESCRIBE TAG people;
DESCRIBE TAG title;

-- 查看某个 Edge 的 schema
DESCRIBE EDGE people_trait;
DESCRIBE EDGE people_familypeople;

-- 查看索引
SHOW TAG INDEXES;
```

### 5.2 按 VID 精确查询（最快，推荐）

```nGQL
-- 获取玩家角色全部属性
FETCH PROP ON people "people.2749760.916505602" YIELD VERTEX AS v;

-- 获取某个 story
FETCH PROP ON story "story.916505602" YIELD VERTEX AS v;

-- 获取某个 title
FETCH PROP ON title "title.k_china.916505602" YIELD VERTEX AS v;
```

### 5.3 LOOKUP（有索引时的过滤查询）

```nGQL
-- 查找 code="strong" 的 trait
LOOKUP ON trait WHERE trait.code == "strong" YIELD VERTEX AS v;

-- 查找特定文化的人（dynasty、isdead 等已建索引的字段）
LOOKUP ON people
  WHERE people.dynasty == 1000103334
    AND people.isdead == false
  YIELD VERTEX AS v
| YIELD properties($-.v).name AS name,
        properties($-.v).age AS age;
```

**注意**：`MATCH WHERE n.story_id == x` 在 people/title/province 等 Tag 上会返回空，因为 story_id 没有索引。用 LOOKUP 或直接按 VID 前缀操作。

### 5.4 MATCH（全局查询，慢但灵活）

```nGQL
-- 统计总数（不带 WHERE，走全表扫描，可以用）
MATCH (n:people) RETURN count(n);    -- 140,390
MATCH (n:trait) RETURN count(n);     -- 444
MATCH (n:story) RETURN count(n);     -- 1

-- 查看所有 story
MATCH (n:story) RETURN n LIMIT 10;

-- 按已索引字段过滤（等价于 LOOKUP）
MATCH (n:people) WHERE n.isdead == false RETURN count(n);
```

### 5.5 GO（图遍历，核心查询）

```nGQL
-- 查询某人的所有特性
GO FROM "people.2749760.916505602" OVER people_trait
  YIELD $$ AS trait_vertex, properties($$).name AS trait_name;

-- 查询某人的家族关系（双向）
GO FROM "people.2749760.916505602" OVER people_familypeople BIDIRECT
  YIELD $$ AS person,
        properties(edge).relation AS relation,
        properties($$).name AS name;

-- 查询某人持有的头衔（反向遍历）
GO FROM "people.2749760.916505602" OVER title_people REVERSELY
  YIELD $$ AS title, properties($$).title_level AS level;

-- 1-2 跳遍历家族
GO 1 TO 2 STEPS FROM "people.2749760.916505602" OVER people_familypeople
  WHERE properties($$).isdead == false
  YIELD DISTINCT $$ AS v;
```

### 5.6 Pipeline 组合查询

```nGQL
-- 找拥有"strong"特性的所有活人（完整模式）
LOOKUP ON trait WHERE trait.code == "strong" YIELD VERTEX AS v
| YIELD id($-.v) AS vid
| GO FROM $-.vid OVER people_trait REVERSELY
    WHERE properties($$).isdead == false
  YIELD $$ AS v, properties($$).name AS name, properties($$).age AS age
| ORDER BY $-.age ASC;
```

---

## 6. VID 格式规则

| Tag | VID 格式 | 示例 |
|-----|---------|------|
| `people` | `people.{id}.{story_id}` | `people.2749760.916505602` |
| `dynasty` | `dynasty.{id}.{story_id}` | `dynasty.1000103334.916505602` |
| `title` | `title.{code}.{story_id}` | `title.k_china.916505602` |
| `province` | `province.{id}.{story_id}` | `province.1174.916505602` |
| `baron` | `baron.{code}.{story_id}` | `baron.b_beijing.916505602` |
| `story` | `story.{story_id}` | `story.916505602` |
| `culture` | `culture.{code}` | `culture.han` |
| `religion` | `religion.{code}` | `religion.taoist` |
| `trait` | `trait.{code}` | `trait.strong` |
| `modifier` | `modifier.{code}` | `modifier.recently_conquered` |
| `building` | `building.{code}` | `building.hillfort_1` |
| `objective` | `objective.{code}` | `objective.obj_become_king` |

**规律**：动态数据（存档派生）带 `story_id` 后缀；静态数据（游戏文件派生）不带。

---

## 7. 当前数据库状态（story_id=916505602）

| 实体 | 数量 | 来源 |
|------|------|------|
| story | 1 | autosave.ck2（2024-11-01） |
| people | 140,390 | 存档文件 |
| trait | 444 | 游戏文件 common/traits/ |
| building | 465 | 游戏文件 common/buildings/ |
| modifier | 1,519 | 游戏文件 common/event_modifiers/ |
| culture | 129 | 游戏文件 common/cultures/ |
| religion | 52 | 游戏文件 common/religions/ |

玩家角色 VID：`people.2749760.916505602`（女皇 傅 珠珠，汉/道教，36岁）

---

## 8. Go 代码连接方式

每个需要连接 Nebula 的模块（`ck2nebula`、`fftanebula`）在模块目录下读取 `nebula-account.yaml`：

```yaml
# nebula-account.yaml（已加入 .gitignore，需手动创建）
address: "localhost:9669"
username: "root"
password: "nebula"
```

`space.go` 初始化模式：

```go
var SPACE *nebulagolang.Space

func init() {
    db, ok := nebulagolang.LoadDB()   // 读取 nebula-account.yaml
    if !ok { log.Fatal("Fail to load database") }
    SPACE = db.Use("ck2")
}
```

连接池最大 300 个连接，批量操作建议 batch=250。

---

## 9. 常见问题

### MATCH WHERE 返回空

```nGQL
-- 这样写会返回空（story_id 无索引）
MATCH (n:people) WHERE n.story_id == 916505602 RETURN count(n);  -- 0

-- 改用 LOOKUP（需索引）或直接按 VID 模式访问
FETCH PROP ON people "people.2749760.916505602" YIELD VERTEX AS v;
```

### 查询很慢

`MATCH (n:people) RETURN count(n)` 需要全表扫描，约 5 秒。有索引的字段用 `LOOKUP ON` 更快。

### 容器不存在

如果 console 容器退出了（exit code 137），直接用 graphd 容器：

```bash
docker exec nebula-docker-compose-graphd-1 \
  /usr/local/nebula/bin/nebula-console \
  -addr localhost -port 9669 -u root -p nebula \
  -e "SHOW SPACES;"
```

---

## 10. 变更记录

| 日期 | 内容 |
|------|------|
| 2026-05-04 | 初始创建，记录 Docker Compose 连接信息、nGQL 查询模式、已知问题 |
