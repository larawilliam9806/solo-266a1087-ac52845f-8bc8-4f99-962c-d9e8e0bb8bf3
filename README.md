# 变异归一化汇总平台

同一次插入/缺失在重复序列中可能被不同样本写成不同坐标。本平台接收参考序列与
逐样本 TSV 变异文件，将等价变异按参考**左对齐归一化并去冗余**，再按规范变异
汇总多样本检出情况。

技术栈：Flask + SQLite（WAL），Docker Compose 一键启动。

## 快速启动

```bash
docker compose up -d --build
# 打开 http://localhost:8000
```

SQLite 数据文件存放在命名卷 `variant-data`（容器内 `/data/variants.db`），
重建容器不丢数据；`SECRET_KEY` 请在 `docker-compose.yml` 中替换为生产密钥。

本地开发：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
DATABASE_PATH=/tmp/dev.db flask --app 'app:create_app()' run --debug
```

## 使用流程

1. **先上传参考序列**：一条仅含大写 `A/C/G/T` 的序列，支持纯文本或单记录 FASTA。
2. **逐样本上传 TSV**：Tab 分隔、1-based 坐标，可带表头：

   ```tsv
   POS	REF	ALT	GT
   3	T	TT	0/1
   8	A	C	1/1
   ```

   - 仅接收：单碱基替换、带锚碱基的纯插入（如 `T>TT`）、带锚碱基的纯缺失（如 `TT>T`）
   - GT 仅允许 `0/1` 或 `1/1`
   - 每行先核对 REF 与当前参考是否一致，再做左对齐归一化
3. 首页按**规范变异**列出检出样本数；点详情查看各样本基因型与原始位点。

## 关键规则

### 归一化（参考左对齐）

采用经典 left-align 流程（与 `bcftools norm` / `vt normalize` 等价）：

1. 去除 REF/ALT 共同后缀；
2. 无法去尾时整体左移一位并补参考前缀碱基，循环直到不能移动；
3. 在参考起点出现空等位基因时回退到最后一个合法带锚状态；
4. 去除共同前缀（每个等位基因至少保留 1 个锚碱基）。

因此重复序列中的等价写法（如 `GCTTTGC` 上 `(3,T,TT)`、`(4,T,TT)`、`(5,T,TT)`）
都会归一化为同一个规范键 `(2,C,CT)`，从而被识别为同一变异。

### 事务与一致性

- 同一样本归一化后出现重复变异（同一规范键）→ **整份文件拒绝**，不写入任何数据。
- 任何校验错误（REF 不符、非法 GT、类型不支持、格式错误等）都在事务内失败回滚，
  **错误上传不改变当前结果**。
- 同名样本重传 = 替换：新文件完整校验通过后，旧变体在同一事务中删除，旧变异不再计数。
- 更换参考采用「重验 + 原子生效」两阶段：
  1. 新参考先登记为**待生效**（同一时间最多一个，第二次换参考会被直接拒绝）；
  2. 逐样本按新参考重新校验，耗时计算在写事务外完成、结果分批暂存；
  3. 全部完成后在一个短事务内原子切换，旧参考降为历史版本。
- **重验期间**页面与变异详情始终展示当前参考下的完整结果；其他窗口仍可按当前
  参考上传、重传或删除样本，不需要等待整批重验结束（不会出现数据库锁错误）。
- 新参考生效时，每个样本按**切换前最后一次成功写入的内容**给出重验结果：
  REF 不符或归一化后重复的样本标记为 `NEEDS_REUPLOAD`（待重传）、变体删除、
  从汇总剔除；重验期间删除的样本不会重新出现。
- 换版失败（校验失败、写锁超时、进程异常等）时**保留原参考**，以及期间已成功
  写入的样本改动；待生效参考及其暂存数据会被丢弃。
- 进程重启时若仍有未完成的待生效参考，按中断处理并回收，不影响当前结果。
- 所有写事务使用 `BEGIN IMMEDIATE` 立即取 SQLite 写锁，使样本上传与参考切换
  严格串行；页面读取使用快照事务，因此看到的始终是**同一版参考下的完整汇总**。


## 目录结构

```
app/
  __init__.py        # Flask 工厂与路由
  normalization.py   # 纯函数：解析、校验、左对齐归一化（无 Flask 依赖）
  db.py              # SQLite 连接、WAL、事务、建表
  services.py        # 上传/重验/替换/汇总业务逻辑
  templates/         # Jinja2 页面
  static/style.css
tests/               # unittest：归一化单测、随机性质测试、端到端集成测试
docker/entrypoint.sh
Dockerfile
docker-compose.yml
```

## 测试

```bash
python -m unittest discover -s tests -v
```

- `test_normalization.py`：解析、边界、等价变异左对齐、重复拒绝等
- `test_properties.py`：4000 个随机变异，验证「单倍型保持」「同事件同规范键」
- `test_app.py`：Flask 端到端流程（上传、替换、换参考重验、错误隔离、删除）
- `test_concurrency.py`：上传/换参考/读取交错时，汇总始终对应单一参考版本
- `test_revalidation.py`：重验期间并发上传/重传/删除、单次换参考互斥、
  失败保留原参考与成功改动、重启回收未完成的待生效参考

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 参考信息、上传表单、样本列表、规范变异汇总 |
| POST | `/reference` | 上传/更换参考（multipart 字段 `reference`） |
| POST | `/samples` | 上传/重传样本（字段 `name`、`sample`） |
| POST | `/samples/<id>/delete` | 删除样本 |
| GET | `/variants/<pos>/<REF>><ALT>` | 变异详情：样本、GT、原始位点 |
| GET | `/healthz` | 健康检查 |
