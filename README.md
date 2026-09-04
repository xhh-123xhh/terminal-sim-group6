# terminal-sim-group6

物联网终端模拟软件（小组 6）

基于 Python 的终端模拟软件集合，按"功能"拆分为独立子目录，每个子目录都可以单独运行。

## 子项目

| 子目录 | 功能 |
| --- | --- |
| [`relay_sim/`](./relay_sim/) | 8 路继电器模拟器：Modbus TCP 从站 + JetLinks MQTT 协议，上报属性、响应平台命令 |
| `temp_humidity_sim/` | （待补）温湿度传感器模拟器 |

> 仓库采用扁平结构，每个子目录是一个**独立可运行**的项目：各自有 `main.py` / 入口、`requirements.txt`、`README.md`。

## 快速开始

```bash
# 进入任一子项目
cd relay_sim

# 安装依赖（推荐 venv）
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/macOS
source venv/bin/activate
pip install -r requirements.txt

# 运行
python main.py
```

## 平台对接说明

两个子项目共用一个 JetLinks 物联网平台（`http://172.16.4.211:8848`）和同一个 MQTT broker
（`172.16.4.211:9783`，账号 `test` / `123456`），但通过**不同产品 ID 和设备 ID** 区分。详细
协议、主题、payload 格式见各自子目录的 README。

## 项目结构

```
terminal-sim-group6/
├── README.md              # 本文件
├── relay_sim/             # 8路继电器模拟器
│   ├── main.py
│   ├── mqtt_client.py
│   ├── modbus_server.py
│   ├── channels.py
│   ├── config.yaml
│   ├── logger.py
│   ├── utils.py
│   ├── requirements.txt
│   ├── README.md
│   ├── test_function_invoke.py    # MQTT 直发测试
│   └── test_platform_invoke.py    # 平台 API 测试
└── temp_humidity_sim/     # （待补）温湿度模拟器
```

## 开发流程与平台凭据

各子项目的 `config.yaml` 内置了**组内**MQTT broker 凭据（`test` / `123456`）。JetLinks
管理员账号（`admin6`）仅在测试脚本中以硬编码方式出现，方便小组成员本地一键复现，**请勿**
将本仓库分享到组外或用于其他用途。