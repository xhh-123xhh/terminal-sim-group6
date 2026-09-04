# 温湿度传感器模拟器（JetLinks MQTT）

按真实温湿度传感器"周期上报 + 响应平台命令"的模型实现的终端模拟器：

- **随机游走**模拟温度 / 湿度，数值越限（死区）才上报，避免噪声
- **JSON 持久化**：重启后从 `sensor_data.json` 恢复上次数值，数据连续
- **MQTT 客户端**（对接 JetLinks 官方 `mqtt-client-gateway` 协议）：属性上报 / 属性写 / 功能调用 / 事件上报
- **越限告警**：温度或湿度超出阈值时通过 `event/alarm` 上报，冷却周期内只报一次

## 架构

```
┌──────────────┐  随机游走生成温湿度  ┌────────────────────────┐
│ 温湿度模拟   │ ───────────────────► │  SensorSimulatorJetLinks │
│ (random walk)│                     │  (状态 + 持久化 + 协议)  │
└──────────────┘                     └───────────┬────────────┘
                                                  │ MQTT (JetLinks 官方协议)
                                                  │  /{pid}/{did}/properties/report (上行)
┌──────────────┐  MQTT (JetLinks 官方协议)         │  /{pid}/{did}/properties/write   (下行)
│ JetLinks 平台│ ◄────────────────────────────────►│  /{pid}/{did}/function/invoke    (下行)
│ 172.16.4.211 │  /{pid}/{did}/function/invoke/reply│  /{pid}/{did}/event/alarm        (上行)
│   :9783      │  /{pid}/{did}/properties/write/reply
└──────────────┘
```

## 关键设计

- **变化上报（死区抑制）**：温度/湿度变化超过 `deadband`（默认 0.2）才上报，
  避免随机游走每周期都发消息，减少 MQTT 流量
- **JSON 原子持久化**：先写 `.tmp` 再 `os.replace`，避免写一半损坏
- **断线自动重连**：`reconnect_delay_set(1~30s)` + 主循环检测 `is_connected()`，
  断线后每次用新时间戳重新生成认证
- **功能/属性写成功后立即补报**：让平台及时看到控制后的最新值

## 目录结构

```
temp_humidity_sim/
├── main.py            # 主程序入口（模拟 + MQTT + 协议处理）
├── config.json        # 配置（product_id/device_id/MQTT/温湿度参数/告警阈值）
├── thing_model.json   # 产品物模型（3 属性 + 3 功能 + 1 事件）
├── requirements.txt   # 依赖
├── README.md          # 本文档
└── test_invoke.py     # 端到端测试：MQTT 直发 → 模拟器 → 回复
```

## 快速开始

### 1. 安装依赖

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
```

### 2. 修改配置

打开 `config.json`，按需修改 `devices.sensor.product_id` 和 `devices.sensor.device_id`
（必须与平台预创建的一致，默认 `th_sensor_6` / `dev_th_6_01`）。

### 3. 启动

```bash
python main.py
```

看到以下日志即成功：

```
[INFO] 传感器模拟器(JetLinks直连)启动: 产品=th_sensor_6 设备=dev_th_6_01 | 温度 27.5°C / 湿度 48.3% | 周期 5s
[INFO] 正在连接 172.16.4.211:9783 ...
[INFO] MQTT 已连接: 172.16.4.211:9783 (clientId=dev_th_6_01)
[INFO] 已订阅: /th_sensor_6/dev_th_6_01/function/invoke / /th_sensor_6/dev_th_6_01/properties/write
[INFO] 属性上报 -> /th_sensor_6/dev_th_6_01/properties/report | 温度=27.6 湿度=48.5 | 变化=['temperature'] (mid=..)
```

## MQTT 协议（JetLinks 官方 4 段式）

所有主题形如 `/{product_id}/{device_id}/<segment>`。

| 方向 | 主题 | 说明 |
| --- | --- | --- |
| 上行 | `properties/report` | 属性上报（变化超过死区时 + 功能/属性写成功后） |
| 上行 | `function/invoke/reply` | 功能调用应答 |
| 上行 | `properties/write/reply` | 属性写应答 |
| 上行 | `event/alarm` | 越限告警事件 |
| 下行 | `function/invoke` | 平台功能调用 |
| 下行 | `properties/write` | 平台属性写 |

### 属性上报 payload（上行）

```json
{
  "messageId": "uuid",
  "timestamp": 1700000000000,
  "properties": {
    "temperature": 27.6,
    "humidity": 48.5,
    "report_interval": 5
  }
}
```

### 功能调用 payload（下行 / 回复）

平台下发（`function/invoke`）：

```json
{
  "messageId": "xxx",
  "functionId": "calibrate_offset",
  "inputs": [{"name": "temp_offset", "value": 2.0}, {"name": "hum_offset", "value": -3.0}]
}
```

> **关键坑**：平台 API/UI 实际下发的 `inputs` 用的是 **`name` 字段**（`{"name":"temp_offset","value":2.0}`），
> 而不是 `id` 字段。模拟器已同时兼容 `name` / `id` / 嵌套 `params` 三种结构；
> 若只按 `id` 解析会导致参数全部取默认值（校准量=0，看似"没生效"）。

模拟器回复（`function/invoke/reply`，注意字段是 `output` 而非 `data`）：

```json
{
  "messageId": "xxx",
  "functionId": "set_report_interval",
  "output": {"interval": 30},
  "success": true,
  "timestamp": 1700000000000
}
```

## 功能定义（function invoke）

产品物模型定义了 3 个功能：

| 功能 ID | 说明 | 输入参数 |
| --- | --- | --- |
| `set_report_interval` | 设置上报周期（秒） | `interval` (int) |
| `calibrate_offset` | 校准温度/湿度偏移（**偏移量持久累加**，作用于显示值） | `temp_offset` (double), `hum_offset` (double) |
| `reset` | 重置温湿度为随机初值，**并清零偏移** | 无 |

> **校准语义**：模拟器内部维护「原始值(raw)」与「偏移(offset)」，`显示值 = 原始值 + 偏移`。
> 随机游走只改原始值，校准只改偏移，所以**校准一次会持续生效**，不会被下一轮随机游走覆盖；
> 偏移量随状态一起持久化到 `sensor_data.json`，重启后不丢失。

### 平台 API 调用

```bash
# 设置上报周期为 10 秒
curl -X POST "http://172.16.4.211:8848/device/instance/dev_th_6_01/function/set_report_interval" \
  -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"interval": 10}'

# 校准偏移（相对当前值累加：温度 +1.0℃、湿度 -2.0%RH，偏移持久生效）
curl -X POST "http://172.16.4.211:8848/device/instance/dev_th_6_01/function/calibrate_offset" \
  -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"temp_offset": 1.0, "hum_offset": -2.0}'

# 重置（温湿度回随机初值，偏移清零）
curl -X POST "http://172.16.4.211:8848/device/instance/dev_th_6_01/function/reset" \
  -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{}'
```

> 注意：平台 API 的 body 是**参数 map**（`{"interval": 10}`）；MQTT 直发时才是 `inputs` 数组。

## 越限告警

温度超出 `[15, 35]℃` 或湿度超出 `[30, 90]%RH` 时，通过 `event/alarm` 上报：

```json
{
  "type": "temperature_high",
  "value": 36.2,
  "message": "温度 36.2℃ 超过上限 35.0℃",
  "timestamp": 1700000000000
}
```

告警有冷却周期（`cooldown_cycles`，默认 6 个上报周期报一次），避免持续越限时刷屏。

## 配置说明（config.json）

| 配置项 | 说明 |
| --- | --- |
| `mqtt.host` / `port` | MQTT broker（默认 `172.16.4.211:9783`） |
| `mqtt.auth_mode` | `none`（本地 EMQX 无认证）或 `md5`（JetLinks 官方鉴权） |
| `mqtt.username` / `password` | MQTT 凭据（`none` 模式下默认 `test/123456`） |
| `devices.sensor.product_id` | 产品 ID（默认 `th_sensor_6`） |
| `devices.sensor.device_id` | 设备 ID（默认 `dev_th_6_01`） |
| `devices.sensor.secure_id` / `secure_key` | `md5` 鉴权时使用（`none` 模式下可忽略） |
| `sensor.interval` | 上报周期（秒，默认 5） |
| `sensor.temp_range` | 温度范围（默认 `[18, 32]`） |
| `sensor.humidity_range` | 湿度范围（默认 `[40, 80]`） |
| `sensor.deadband` | 变化死区（默认 0.2，超过才上报） |
| `sensor.max_step` | 随机游走最大步长（默认 0.5） |
| `sensor.alarm.*` | 越限告警阈值与冷却周期 |

## 测试

仓库自带端到端测试脚本，验证完整链路（MQTT 直发 → 模拟器 → 回复）：

```bash
python test_invoke.py
```

## 在 JetLinks 平台创建产品 / 设备

1. 登录 JetLinks 平台（`http://172.16.4.211:8848`）
2. 设备管理 → 产品 → 新建产品（消息协议选 **MQTT**），产品 ID 填 `th_sensor_6`
3. 进入产品详情，按 `thing_model.json` 定义物模型（3 属性 + 3 功能 + 1 事件）
4. 设备管理 → 设备 → 新建设备，设备 ID 填 `dev_th_6_01`
5. 把 `config.json` 里的 `product_id` / `device_id` 与平台保持一致
6. **重要**：确认产品的**存储策略**为 `timescaledb-row`（默认 `none` 会导致属性不入库、`properties/latest` 为空）

## 常见问题

**Q: 平台收不到上报？**
A: 1) 检查 `config.json` 里的 `product_id`/`device_id` 是否与平台一致；
   2) 检查 MQTT broker 是否可达；3) 启动后看 `MQTT 已连接` 是否成功。

**Q: 设备在线但属性为空？**
A: 检查产品的**存储策略**是否为 `timescaledb-row`（必须显式设置，默认 `none` 不入库）。

**Q: 功能调用没反应？**
A: 1) 看日志是否 `收到功能调用`；2) 确认 `functionId` 与物模型一致；
   3) MQTT 直发时用 `inputs` 数组格式，平台 API 用参数 map 格式。
