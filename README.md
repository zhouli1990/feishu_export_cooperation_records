# 飞书合同协同记录批量拉取

批量按时间窗口调用飞书合同协同“导出记录”接口，并通过 Playwright 自动化从飞书网页版会话中点击“下载文件”，最终将分批 Excel 合并为一份总表。支持断点续跑、按 7→3→1 天自适应细分窗口、日志与状态留存。

- **入口脚本**：`scripts/export_runner.py`
- **默认配置**：`config/config.yaml`
- **输出目录**：`output/raw`（分批）与 `output/merged`（汇总）
- **状态追踪**：`state/run_windows.csv`

---

## 环境要求

- Python 3.10+（Windows/macOS/Linux 均可，推荐 Windows）
- 依赖安装（见 `requirements.txt`）：
  - requests, pyyaml, pandas, openpyxl, playwright, tqdm
- 首次使用 Playwright 需要安装浏览器内核

```powershell
# 安装依赖
pip install -r requirements.txt

# 安装 Playwright 浏览器内核（仅首次）
playwright install
```

> 若 `playwright` 命令不可用，可用 `python -m playwright install`。

---

## 目录结构

```
./
  config/
    config.yaml                # 运行配置
  scripts/
    export_runner.py           # 入口：调度+编排+合并
    orchestrator.py            # 时间窗口细分与执行主流程
    http_export.py             # 导出接口封装（HTTP）
    web_download.py            # Playwright 自动化下载
    merge_and_validate.py      # 合并 Excel（按 with_data 窗口）
    run_state.py               # 运行状态 CSV 辅助
    window_gen.py              # 初始窗口生成
  output/
    raw/                       # 分窗口原始 Excel
    merged/                    # 汇总输出
  state/
    run_windows.csv            # 运行状态（断点续跑“事实源”）
  logs/                        # 运行日志
  requirements.txt
  README.md
```

---

## 快速开始

1. 拉取/解压本项目，安装依赖并安装 Playwright 浏览器内核。
2. 编辑 `config/config.yaml`：
   - 设置 `start_date`、`end_date` 时间范围（含端点）。
   - 在 `export_headers.cookie` 填入你通过浏览器抓包获取到的 Cookie。
   - 按需调整下载目录、机器人会话名称等。
3. 运行：

```powershell
python scripts/export_runner.py -c config/config.yaml
```

运行后：
- 控制台与 `logs/run_*.log` 输出详细日志。
- 自动打开（或复用）Chromium 窗口，进入飞书网页版消息；首次需要扫码登录。
- 下载的 Excel 存在 `output/raw` 下，命名为 `合同协同_YYYYMMDD-YYYYMMDD_共{COUNT}条.xlsx`。
- 所有 `with_data` 窗口最终合并至 `output/merged/`，文件名包含总条数。

---

## 配置说明（与当前实现保持一致）

`config/config.yaml` 关键项：

- **start_date / end_date**：任务起止日期（含），格式 `YYYY-MM-DD`。
- **split_days_sequence**：窗口细分序列，命中阈值后按 7→3→1 逐级拆分。
- **max_count_per_file**：导出单文件上限（通常为 1000）。当机器人消息“共计”== 上限时触发细分。
- **retry**：HTTP 导出重试策略（请求异常/非 2xx）。
- **export_headers**：导出接口请求头。当前版本实际使用字段：
  - `timezone_offset`（示例：-480 表示 UTC+8）
  - `cookie`（从抓包复制整段 Cookie）
  - `content_type`（默认 `application/json`）
  - 注：若后端策略变化需要额外头（如 csrf-token），请相应扩展 `scripts/http_export.py`。
- **download**（Playwright）：
  - `user_data_dir`：浏览器用户目录（持久化登录态，减少重复扫码）。
  - `download_dir`：下载目录（相对项目根）。
  - `bot_chat_name`：机器人会话名称（例如“飞书合同”）。
  - `max_wait_seconds`：等待新消息的最大时长（默认 90s）。
- **merge.output_path_pattern**：合并结果路径模板，`{TOTAL}` 会替换为累计条数。
- **run_state**：运行状态 CSV 输出及断点续跑参数。
  - `csv_path`、`encoding`（默认 utf-8-sig 便于 Excel）
  - `line_ending`（Windows 推荐 crlf）
  - `flush`（每条立即落盘）
  - `resume_mode`：`resume`（默认，仅补缺口）| `full`（全量重跑）
  - `completed_statuses`：视为“已完成”的状态集合（默认 `with_data`/`no_data`/`manual`）
- **log.level**：日志级别（DEBUG/INFO/WARN/ERROR）。

示例片段（敏感字段请替换为你自己的）：

```yaml
start_date: "2025-03-01"
end_date: "2025-10-01"
split_days_sequence: [7, 3, 1]
max_count_per_file: 1000
retry:
  max_attempts: 3
  backoff_seconds: 5
export_headers:
  timezone_offset: -480
  cookie: "<你的抓包 Cookie>"
  content_type: "application/json"
download:
  user_data_dir: "./.browser_profile"
  download_dir: "./output/raw"
  bot_chat_name: "飞书合同"
  max_wait_seconds: 90
merge:
  output_path_pattern: "./output/merged/合同协同_merge_共{TOTAL}条.xlsx"
run_state:
  csv_path: "./state/run_windows.csv"
  encoding: "utf-8-sig"
  line_ending: "crlf"
  flush: true
  resume_mode: "resume"
  completed_statuses: ["with_data", "no_data", "manual"]
log:
  level: "DEBUG"
```

---

## 执行流程（简述）

- **窗口生成**：按 `split_days_sequence[0]`（默认 7 天）在 `[start_date, end_date]` 范围内生成初始闭区间窗口。
- **提交导出**：对每个窗口调用导出接口（`scripts/http_export.py`）。
- **等待并下载**：Playwright 监听“下载文件”按钮出现并点击保存；解析同条消息中的“共计：XXX”。
- **自适应细分**：若 `declared_count == max_count_per_file` 且窗口天数>1，则按序列拆分为更小窗口继续；若已至 1 天仍等于上限，则标记 `manual`。
- **状态记录**：每个窗口在 `state/run_windows.csv` 中写入一条最终状态：
  - `with_data` / `no_data` / `failed` / `manual`
- **合并输出**：仅将 `with_data` 窗口对应的 Excel 参与合并，生成最终汇总文件。

CSV 字段（列头）参见 `scripts/run_state.py`：

```
window_id,from_date,to_date,window_days,status,declared_count,split_level,retries,skip_reason,exception,file_path,file_md5,start_time,end_time,duration_ms
```

---

## 断点续跑与重跑

- 默认 `resume_mode=resume`：读取 `csv_path`，跳过 `completed_statuses` 中的窗口，仅执行缺失/失败窗口。
- 全量重跑方式：
  - 将 `resume_mode` 设为 `full`，或
  - 备份/清空 `state/run_windows.csv` 后重跑（不推荐直接删除历史，建议备份）。

---

## 常见问题（FAQ）

- **首次运行需要扫码吗？**
  - 需要。首跑会打开可视化浏览器扫码登录；配置了 `user_data_dir` 后可复用登录态。

- **导出接口 401/403？**
  - 更新 `export_headers.cookie` 为最新抓包值；确保 `Timezone-Offset`、`Content-Type` 正确。

- **页面有“导出完成”消息但未自动下载？**  
  - 检查 `bot_chat_name` 是否匹配；或在浏览器中手动点击到对应会话后重试。

- **明明超过 1000 条为何没有继续细分？**
  - 当前逻辑以机器人消息中的“共计：XXX”作为判断依据；若文案结构变化，请根据实际页面更新 `web_download.py` 的解析逻辑。

- **Playwright 提示未安装浏览器内核**
  - 执行 `playwright install` 或 `python -m playwright install`。

---

## 安全与合规

- 配置中的 Cookie 属个人敏感信息，请勿提交到版本库或传播给无关人员。
- 默认 `.gitignore` 已忽略常见输出目录；请自行确认不要提交含敏感信息的文件。

---

## 开发者速览

- 关键模块：
  - `scripts/orchestrator.py`：窗口切分与整体编排
  - `scripts/http_export.py`：导出接口封装与重试
  - `scripts/web_download.py`：Playwright 自动化下载与“共计”解析
  - `scripts/merge_and_validate.py`：`with_data` 文件合并
  - `scripts/run_state.py`：状态 CSV 维护
  - `scripts/window_gen.py`：初始窗口生成

---

## 免责声明

本脚本用于内部业务数据整理与归档，请遵守所在组织与平台的使用条款与数据合规要求。使用者自行承担因使用本工具产生的风险与责任。
