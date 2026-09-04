"""8路继电器模拟器主程序

功能：
- 启动 Modbus TCP 从站（监听本地端口，地址 6 = 8 路继电器 bit 字段）
- 启动 MQTT 客户端（连接 JetLinks 平台，上报/接收命令）
- 内部状态中枢：ChannelManager（线程安全）
- 状态变化自动同步：
    * Modbus 写 → channels 变化 → MQTT 上报 + 状态回灌寄存器
    * MQTT 命令 → channels 变化 → 下次 Modbus 读返回新值
- 定时上报心跳
"""
import signal
import sys
import time
from pathlib import Path

import yaml

from channels import ChannelManager
from logger import get_logger, setup_logger
from modbus_server import ModbusServer
from mqtt_client import JetLinksMQTTClient
from utils import now_ts


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    # 默认配置文件
    cfg_path = Path(__file__).parent / "config.yaml"
    cfg = load_config(str(cfg_path))

    # 初始化日志
    log_cfg = cfg.get("logger", {})
    log_file = log_cfg.get("file")
    if log_file and not Path(log_file).is_absolute():
        log_file = str(Path(__file__).parent / log_file)
    log = setup_logger(
        name="simulator",
        level=log_cfg.get("level", "INFO"),
        log_file=log_file,
        max_bytes=int(log_cfg.get("max_bytes", 10 * 1024 * 1024)),
        backup_count=int(log_cfg.get("backup_count", 5)),
    )

    log.info("=" * 60)
    log.info("8路继电器模拟器启动")
    log.info("product_id=%s device_id=%s",
             cfg["device"]["product_id"], cfg["device"]["device_id"])
    log.info("=" * 60)

    # ---------- 初始化 channel ----------
    ch_cfg = cfg["channels"]
    channels = ChannelManager(
        nbits=ch_cfg["count"],
        initial=ch_cfg.get("initial_states"),
        bit_map=ch_cfg.get("modbus", {}).get("bit_map"),
    )
    channels.bind_logger(log)

    # ---------- 启动 Modbus server ----------
    modbus = ModbusServer(cfg["modbus_server"], channels)
    modbus.start()

    # ---------- 启动 MQTT client ----------
    mqtt = JetLinksMQTTClient(cfg, channels)

    # 注入命令处理器：收到 MQTT 命令 → 更新 channels → 触发 on_change → 立即上报
    def on_command(payload: dict, msg_id: str):
        log.info("command received: id=%s payload=%s", msg_id, payload)
        changed, _ = channels.set_channels_from_dict(payload, source="mqtt")
        # 响应
        mqtt.publish_set_reply(msg_id, code=200, msg="ok")
        # on_change 已经会触发立即上报，无需这里重复

    mqtt.set_command_handler(on_command)
    mqtt.connect()

    # ---------- 注册 channel 变化回调：状态变化时立即上报 ----------
    def on_state_change(old: int, new: int, changed: int):
        log.info("state changed: 0x%02X -> 0x%02X (mask=0x%02X)", old, new, changed)
        if mqtt.is_connected():
            mqtt.publish_property()

    channels.on_change(on_state_change)

    # ---------- 优雅退出 ----------
    stop_flag = {"stop": False}

    def _on_signal(signum, frame):
        log.info("received signal %s, shutting down...", signum)
        stop_flag["stop"] = True

    try:
        signal.signal(signal.SIGINT, _on_signal)
        signal.signal(signal.SIGTERM, _on_signal)
    except Exception:
        # Windows 下 SIGTERM 可能不工作
        pass

    # ---------- 主循环：定时上报 ----------
    report_cfg = cfg["reporter"]
    interval = float(report_cfg.get("interval_seconds", 5))
    heartbeat = float(report_cfg.get("heartbeat_seconds", 30))
    last_hb = 0.0

    log.info("entering main loop: report_interval=%.1fs heartbeat=%.1fs", interval, heartbeat)

    try:
        while not stop_flag["stop"]:
            time.sleep(interval)
            if not mqtt.is_connected():
                continue
            now = time.time()
            # 状态上报
            mqtt.publish_property()
            # 心跳（每 heartbeat 秒重发 online）
            if now - last_hb >= heartbeat:
                mqtt.publish_online()
                last_hb = now
    except KeyboardInterrupt:
        log.info("interrupted by user")
    finally:
        log.info("shutting down...")
        mqtt.stop()
        modbus.stop()
        log.info("bye.")


if __name__ == "__main__":
    main()