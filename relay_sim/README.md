# 8路继电器模拟器（Modbus TCP + JetLinks MQTT）

按真实设备"被平台控制"的模型实现的 8 路继电器终端模拟器：

- **Modbus TCP 从站**（`0.0.0.0:5502`）：对外提供寄存器接口，方便用 Modbus Poll / 其他主站验证状态
- **MQTT 客户端**（对接 JetLinks 官方 `mqtt-client-gateway` 协议）：属性上报 / 命令响应 / 功能调用
- **唯一事实源 `ChannelManager`**：所有读写都过这里，状态变化时通过回调自动触发上报

## 架构

```
┌──────────────┐  Modbus TCP (reg 6 = 8ch bit field)  ┌──────────────────┐
│ 外部主站     │ ◄────────────────────────────────────► │  本模拟器        │
│ (Modbus Poll)│                                       │  Modbus TCP 从站 │
└──────────────┘                                       │  :5502           │
                                                       └────────┬─────────┘
                                                                │ 状态同步
                                                       ┌────────▼─────────┐
┌──────────────┐  MQTT (JetLinks 官方协议)               │  ChannelManager  │
│ JetLinks 平台│ ◄────────────────────────────────────► │  (线程安全中枢)  │
│ 172.16.4.211 │   /{pid}/{did}/properties/report (上行)  └──────────────────┘
│   :9783      │   /{pid}/{did}/properties/write   (下行)
│ (经 EMQX)    │   /{pid}/{did}/properties/write/reply
└──────────────┘   /{pid}/{did}/function/invoke      (下行)
                   /{pid}/{did}/function/invoke/reply (上行)
                   /{pid}/{did}/online | /offline
```

## 关键设计

- **寄存器映射**：Modbus Holding Register 地址 6（0x0006）的低 8 bit 表示 8 路继电器状态
  - `bit0 = 通道1, bit1 = 通道2, ..., bit7 = 通道8`
  - `0 = 关（OFF），1 = 开（ON）`
  - 例：寄存器值 = `0xA5` = `0b10100101` → 通道 1/3/6/8 ON
- **唯一事实源**：ChannelManager（线程安全）
  - 所有 Modbus 读写、MQTT 收发都更新/读取这里
  - 状态变化时通过回调自动触发 MQTT 上报
- **Modbus 寄存器是 ChannelManager 的"实时投影"**
  - 读请求：从 ChannelManager 读最新值返回（避免长轮询读到旧值）
  - 写请求：解析后写入 ChannelManager

## 目录结构

```
relay_sim/
├── main.py                # 主程序入口（启动 Modbus + MQTT 客户端）
├── channels.py               # 8路继电器状态管理（核心）
├── modbus_server.py          # Modbus TCP 从站（pymodbus 3.15+ simulator API）
├── mqtt_client.py            # JetLinks MQTT 客户端（paho-mqtt 2.x）
├── config.yaml               # 配置文件（product_id/device_id/MQTT/Modbus 等）
├── logger.py                 # 日志（统一 root logger）
├── utils.py                  # 工具函数（位运算/JSON/时间戳）
├── requirements.txt                 # 依赖
├── README.md                 # 本文档
├── test_function_invoke.py   # 端到端测试：MQTT 直发 → 模拟器 → 回复
└── test_platform_invoke.py   # 端到端测试：平台 API → MQTT → 模拟器 → 回复
```

## 快速开始

### 1. 安装依赖

```bash
# 推荐用 venv
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
```

### 2. 修改配置

打开 `config.yaml`，按需修改 `device.product_id` 和 `device.device_id`（必须与平台预创建的一致）。

### 3. 启动

```bash
python main.py
```

看到以下日志即成功：

```
[simulator] 8路继电器模拟器启动
[modbus] modbus server started on 0.0.0.0:5502 (reg 6 = 8ch relays)
[mqtt] connecting to mqtt://172.16.4.211:9783
[mqtt] mqtt connected
[mqtt] subscribed: /relay-8ch-group6/relay-sim-group6-001/properties/write | /relay-8ch-group6/relay-sim-group6-001/function/invoke
[simulator] entering main loop
```

### 4. 验证

#### 用 Modbus 客户端脚本

```python
from pymodbus.client import ModbusTcpClient

c = ModbusTcpClient("127.0.0.1", port=5502)
c.connect()
# 写 0xA5 (10100101): ch1/3/6/8 ON
c.write_register(6, 0xA5, device_id=1)
# 读
print(c.read_holding_registers(6, count=1, device_id=1).registers)
c.close()
```

或者用任意 Modbus 主站工具（如 Modbus Poll）连接 `127.0.0.1:5502`，Slave ID = 1，监控 Holding Register 6。

#### 用平台 API 下发属性

```bash
TOKEN=<your_jetlinks_token>
curl -X PUT "http://172.16.4.211:8848/device/instance/relay-sim-group6-001/property" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"ch1_state": true, "ch3_state": true, "ch5_state": true}'
```

设备收到后：更新内部状态 → 同步到 Modbus 寄存器 6 → 上报最新状态到 `properties/report`。

## MQTT 协议（JetLinks 官方 4 段式）

所有主题模板在 `config.yaml -> mqtt.topics`，运行时会替换为：
`/{product_id}/{device_id}/<segment>`。

| 方向 | 主题 | 说明 |
| --- | --- | --- |
| 上行 | `properties/report` | 属性上报（默认 5s 周期 + 变化时立即上报） |
| 上行 | `online` / `offline` | 设备上下线通知（默认 30s 心跳） |
| 下行 | `properties/write` | 平台下发属性写命令 |
| 下行 | `function/invoke` | 平台下发功能调用 |
| 上行 | `properties/write/reply` | 对属性写命令的应答 |
| 上行 | `function/invoke/reply` | 对功能调用的应答 |

### 属性上报 payload（上行）

```json
{
  "timestamp": 1700000000000,
  "messageId": "uuid",
  "properties": {"ch1_state": true, "ch2_state": false, "...", "ch8_state": false, "online": true}
}
```

### 属性写命令 payload（下行）

```json
{
  "timestamp": 1700000000000,
  "messageId": "xxx",
  "properties": {"ch1_state": true, "ch3_state": true, "ch5_state": true}
}
```

模拟器处理后回：

```json
{"timestamp": ..., "messageId": "xxx", "success": true, "code": 200, "msg": "ok"}
```

## 功能定义（function invoke）

产品物模型除了 8 个属性（`ch1_state~ch8_state`），还定义了一个功能 `set`「设置8路继电器」
（8 个 boolean 输入参数 `ch1~ch8`），用于在 JetLinks 界面「设备功能」tab 直接下发控制。

### 平台 API 调用

```bash
curl -X POST "http://172.16.4.211:8848/device/instance/relay-sim-group6-001/function/set" \
  -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"ch2": true, "ch4": true}'
```

> 注意：body 是**参数 map**（`{"ch2": true}`），不要包 `{"inputs": [...]}`，否则平台会把
> 整个 `inputs` 当成一个参数（返回 `no channel params`）。

### 下行 payload（下行）

```json
{
  "messageId": "xxx",
  "deviceId": "relay-sim-group6-001",
  "functionId": "set",
  "inputs": [{"name": "ch1", "value": true}, {"name": "ch3", "value": true}]
}
```

### 上行回复 payload

```json
{
  "messageId": "xxx",
  "functionId": "set",
  "output": {"ch1_state": true, "ch2_state": false, "...", "ch8_state": false},
  "success": true
}
```

模拟器 `mqtt_client.py` 会解析 `inputs` 数组（兼容 `{name: value}` 对象），把 `ch1` / `ch1_state`
统一映射成 `ch1_state`，写入 Modbus 寄存器 6，然后回复当前 8 路状态。

## 配置说明（config.yaml）

| 配置项 | 说明 |
| --- | --- |
| `device.product_id` | JetLinks 产品 ID（默认 `relay-8ch-group6`） |
| `device.device_id` | JetLinks 设备 ID（默认 `relay-sim-group6-001`） |
| `device.secure_key` | 设备密钥（用 secureKey 鉴权时需要） |
| `channels.count` | 继电器通道数（默认 8） |
| `channels.modbus.register_address` | 继电器状态寄存器地址（默认 6） |
| `channels.modbus.bit_map` | 通道到位掩码的映射（默认 `bit0=ch1 ... bit7=ch8`） |
| `channels.initial_states` | 启动初始状态 |
| `modbus_server.host` / `port` | Modbus TCP 监听地址（默认 `0.0.0.0:5502`） |
| `mqtt.broker` / `port` | MQTT broker（默认 `172.16.4.211:9783`） |
| `mqtt.username` / `password` | MQTT 凭据（默认 `test/123456`） |
| `mqtt.topics.*` | JetLinks 主题模板 |
| `reporter.interval_seconds` | 定时上报周期（默认 5s） |
| `reporter.heartbeat_seconds` | 平台心跳周期（默认 30s） |
| `logger.level` | 日志级别（`DEBUG` / `INFO` / `WARNING` / `ERROR`） |
| `logger.file` | 日志文件路径（`logs/simulator.log`） |

## 验证清单

| 项目 | 验证方式 |
| --- | --- |
| 设备上线平台 | 启动后看 JetLinks 设备列表状态=在线；MQTTX 看 `/{pid}/{did}/online` |
| 远程控制 8 路 | 平台/APP 切换按钮，看 Modbus 寄存器 6 的值变化 |
| 命令/设备日志一致 | 启动 DEBUG 级别日志，对比时间戳 |
| 网络断开自动重连 | 关闭 broker → 看 `[mqtt] disconnected` → 重新启动 broker → `[mqtt] mqtt connected` |

## 测试

仓库自带两个端到端测试脚本，会在真实环境上验证完整链路：

```bash
# 模拟平台下发：MQTT 直发 function/invoke → 验证模拟器写入寄存器 + 回复
python test_function_invoke.py

# 走平台 API：登录平台 → POST function/set → 验证平台下发 + 模拟器回复
python test_platform_invoke.py
```

## 在 JetLinks 平台创建产品 / 设备

模拟器要能上报/收命令，必须在 JetLinks 平台预先创建产品和设备：

1. 登录 JetLinks 平台（`http://<jetlinks-host>:9000`）
2. 设备管理 → 产品 → 新建产品（消息协议选 **MQTT**）
3. 进入产品详情，定义物模型（8 个属性 `ch1_state~ch8_state`，bool）+ 1 个功能 `set`（8 个 bool 输入 `ch1~ch8`）
4. 设备管理 → 设备 → 新建设备（所属产品：上一步的产品）
5. 把产品的 `productId` 和设备的 `deviceId` 填到 `config.yaml` 的 `device.product_id` / `device.device_id`
6. **重要**：确认产品的**存储策略**为 `timescaledb-row`（默认 `none` 会导致属性不入库、`properties/latest` 为空）

## 常见问题

**Q: 端口 5502 被占用？**
A: 修改 `config.yaml` 的 `modbus_server.port`，重启程序。

**Q: 平台收不到消息？**
A: 1) 检查 `config.yaml` 里的 `product_id`/`device_id` 是否与平台一致；
   2) 检查 MQTT broker 是否可达；3) 启动后看 `[mqtt] mqtt connected` 是否成功。

**Q: 设备在线但属性一直为空？**
A: 检查产品的**存储策略**是否为 `timescaledb-row`（必须显式设置，默认 `none` 不入库）。
**Q: 命令下发没反应？**
A: 1) 启动 DEBUG 日志，看 `[mqtt] recv topic=...` 是否收到；
   2) 检查命令 payload 格式（属性写必须有 `properties.ch*_state`；功能调用必须带 `inputs` 数组）。