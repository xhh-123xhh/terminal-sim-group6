#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
温湿度传感器模拟器 - JetLinks 物模型直连版
===========================================
模拟真实温湿度传感器终端：随机游走生成温度 / 湿度数据，通过 MQTT 对接
JetLinks 物联网平台官方物模型协议。

对接 JetLinks 官方 MQTT 协议:
    属性上报:  /{productId}/{deviceId}/properties/report   {"temperature":25.6,"humidity":60.2}
    功能下发:  /{productId}/{deviceId}/function/invoke     (平台->设备, 订阅)
    功能回复:  /{productId}/{deviceId}/function/invoke/reply
    属性修改:  /{productId}/{deviceId}/properties/write    (平台->设备, 订阅)
    属性回复:  /{productId}/{deviceId}/properties/write/reply
    事件上报:  /{productId}/{deviceId}/event/{eventId}

认证 (JetLinks 官方协议, auth_mode=md5):
    clientId = 设备ID
    username = secureId | 毫秒时间戳
    password = md5(secureId | 毫秒时间戳 | secureKey)

本地测试时把 mqtt.auth_mode 设为 "none" 即可连本地 EMQX 无认证 broker。
"""

import hashlib
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime

import paho.mqtt.client as mqtt

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def _resolve_config_path():
    for base in (SCRIPT_DIR, os.path.dirname(SCRIPT_DIR)):
        path = os.path.join(base, "config.json")
        if os.path.exists(path):
            return path
    return os.path.join(SCRIPT_DIR, "config.json")


CONFIG_PATH = _resolve_config_path()
BASE_DIR = os.path.dirname(CONFIG_PATH)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sensor-jetlinks")


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def jetlinks_credentials(cfg, device_key):
    """JetLinks 官方 MQTT 认证: username=secureId|ts, password=md5(secureId|ts|secureKey)"""
    m = cfg["mqtt"]
    if m.get("auth_mode") == "md5":
        dev = cfg["devices"][device_key]
        ts = int(time.time() * 1000)
        username = "%s|%d" % (dev["secure_id"], ts)
        password = hashlib.md5(
            ("%s|%d|%s" % (dev["secure_id"], ts, dev["secure_key"])).encode("utf-8")
        ).hexdigest()
        return username, password
    if m.get("username"):
        return m["username"], m.get("password") or ""
    return None, None


class SensorSimulatorJetLinks:
    """温湿度模拟 + JSON持久化 + JetLinks 物模型协议上报/功能下发"""

    def __init__(self, cfg):
        self.cfg = cfg
        dev = cfg["devices"]["sensor"]
        self.product_id = dev["product_id"]
        self.device_id = dev["device_id"]

        scfg = cfg["sensor"]
        self.json_file = os.path.join(BASE_DIR, scfg.get("json_file", "sensor_data.json"))
        self.interval = scfg.get("interval", 5)
        self.t_min, self.t_max = scfg.get("temp_range", [18.0, 32.0])
        self.h_min, self.h_max = scfg.get("humidity_range", [40.0, 80.0])
        self.deadband = scfg.get("deadband", 0.2)
        self.max_step = scfg.get("max_step", 0.5)
        alarm = scfg.get("alarm", {})
        self.alarm_cfg = {
            "temp_min": alarm.get("temp_min", 15.0),
            "temp_max": alarm.get("temp_max", 35.0),
            "hum_min": alarm.get("hum_min", 30.0),
            "hum_max": alarm.get("hum_max", 90.0),
            "cooldown": alarm.get("cooldown_cycles", 6),
        }

        # JetLinks 官方物模型主题
        base = "/%s/%s" % (self.product_id, self.device_id)
        self.topic_report = base + "/properties/report"
        self.topic_fn_invoke = base + "/function/invoke"
        self.topic_fn_reply = base + "/function/invoke/reply"
        self.topic_prop_write = base + "/properties/write"
        self.topic_prop_write_reply = base + "/properties/write/reply"
        self.topic_event_alarm = base + "/event/alarm"
        self.qos = cfg["mqtt"].get("qos", 1)

        self.seq = 0
        self.alarm_cycle = 0
        self.running = True

        state = self._load_state()
        self.temperature = state["temperature"]
        self.humidity = state["humidity"]

        self.mqtt = None

    # ---------- JSON 持久化 ----------
    def _load_state(self):
        if os.path.exists(self.json_file):
            try:
                with open(self.json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return {
                    "temperature": float(data["temperature"]),
                    "humidity": float(data["humidity"]),
                }
            except Exception as e:
                log.warning("JSON 状态文件损坏, 使用随机初值: %s", e)
        return {
            "temperature": round(random.uniform(self.t_min, self.t_max), 1),
            "humidity": round(random.uniform(self.h_min, self.h_max), 1),
        }

    def _save_state(self):
        payload = {
            "device_id": self.device_id,
            "product_id": self.product_id,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "temperature": round(self.temperature, 1),
            "humidity": round(self.humidity, 1),
        }
        tmp = self.json_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.json_file)

    # ---------- MQTT ----------
    def _make_mqtt_client(self):
        mcfg = self.cfg["mqtt"]
        username, password = jetlinks_credentials(self.cfg, "sensor")

        def on_connect(client, userdata, flags, reason_code, properties=None):
            if reason_code == 0:
                log.info("MQTT 已连接: %s:%s (clientId=%s)", mcfg["host"], mcfg["port"], self.device_id)
                client.subscribe(self.topic_fn_invoke, qos=1)
                client.subscribe(self.topic_prop_write, qos=1)
                log.info("已订阅: %s / %s", self.topic_fn_invoke, self.topic_prop_write)
            else:
                log.warning("MQTT 连接失败, reason_code=%s", reason_code)

        def on_disconnect(client, userdata, flags, reason_code=None, properties=None):
            log.warning("MQTT 连接断开 (rc=%s), 将自动重连", reason_code)

        if hasattr(mqtt, "CallbackAPIVersion"):
            client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.device_id,
                clean_session=True,
            )
        else:
            client = mqtt.Client(client_id=self.device_id, clean_session=True)
        if username:
            client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        return client

    def _connect(self):
        try:
            self.mqtt = self._make_mqtt_client()
            self.mqtt.connect_async(self.cfg["mqtt"]["host"], self.cfg["mqtt"]["port"],
                                    self.cfg["mqtt"].get("keepalive", 60))
            self.mqtt.loop_start()
            log.info("正在连接 %s:%s ...", self.cfg["mqtt"]["host"], self.cfg["mqtt"]["port"])
        except Exception as e:
            log.error("MQTT 初始连接异常: %s", e)

    def _teardown(self):
        try:
            self.mqtt.loop_stop()
            self.mqtt.disconnect()
        except Exception:
            pass
        self.mqtt = None

    # ---------- 消息处理 ----------
    def _on_message(self, client, userdata, msg):
        text = msg.payload.decode("utf-8", "ignore")
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            log.warning("收到非 JSON 消息: %r", text)
            return
        if msg.topic.endswith("/function/invoke"):
            self._handle_function_invoke(data)
        elif msg.topic.endswith("/properties/write"):
            self._handle_property_write(data)
        else:
            log.info("忽略未知主题消息: %s", msg.topic)

    def _handle_function_invoke(self, msg):
        """平台功能调用:
        兼容两种 inputs 结构:
          平铺:    [{"id":"interval","value":30}]
          嵌套:    [{"name":"params","value":[{"id":"interval","value":30}]}]
        """
        mid = msg.get("messageId")
        fid = msg.get("functionId")
        raw_inputs = msg.get("inputs") or []
        if raw_inputs and isinstance(raw_inputs[0], dict) and raw_inputs[0].get("name") == "params":
            raw_inputs = raw_inputs[0].get("value") or []
        inputs = {i.get("id"): i.get("value") for i in raw_inputs if isinstance(i, dict)}
        log.info("收到功能调用: %s inputs=%s", fid, inputs)

        success, data = False, {"error": "unknown function: %s" % fid}
        if fid == "set_report_interval":
            v = max(1, int(inputs.get("interval", self.interval)))
            self.interval = v
            success, data = True, {"interval": v}
        elif fid == "calibrate_offset":
            self.temperature = round(max(self.t_min, min(self.t_max,
                self.temperature + float(inputs.get("temp_offset", 0)))), 1)
            self.humidity = round(max(self.h_min, min(self.h_max,
                self.humidity + float(inputs.get("hum_offset", 0)))), 1)
            self._save_state()
            success, data = True, {"temperature": self.temperature, "humidity": self.humidity}
        elif fid == "reset":
            self.temperature = round(random.uniform(self.t_min, self.t_max), 1)
            self.humidity = round(random.uniform(self.h_min, self.h_max), 1)
            self._save_state()
            success, data = True, {"temperature": self.temperature, "humidity": self.humidity}
        else:
            log.warning("未知功能: %s", fid)

        self._reply(self.topic_fn_reply, mid, success, data)
        if success:
            # 功能执行成功后立即补发一次属性上报,
            # 让平台能及时看到控制后的最新值(尤其 set_report_interval 这类不改数值的功能)
            self._publish_report(changed="after-fn:%s" % fid)

    def _handle_property_write(self, msg):
        """平台修改属性: 兼容 dict 与 list 两种结构
        {"messageId":"..","properties":{"report_interval":10}}
        {"messageId":"..","properties":[{"id":"report_interval","value":10}]}
        """
        mid = msg.get("messageId")
        props = msg.get("properties") or {}
        if isinstance(props, list):
            props = {i.get("id"): i.get("value") for i in props if isinstance(i, dict)}
        applied = {}
        if "report_interval" in props:
            self.interval = max(1, int(props["report_interval"]))
            applied["report_interval"] = self.interval
        if "temperature" in props:
            self.temperature = round(float(props["temperature"]), 1)
            applied["temperature"] = self.temperature
        if "humidity" in props:
            self.humidity = round(float(props["humidity"]), 1)
            applied["humidity"] = self.humidity
        if applied:
            self._save_state()
            log.info("属性修改已应用: %s", applied)
        self._reply(self.topic_prop_write_reply, mid, True, applied)
        if applied:
            self._publish_report(changed="after-write")

    def _reply(self, topic, message_id, success, data):
        if not message_id:
            return
        payload = {
            "messageId": message_id,
            "success": success,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        self.mqtt.publish(topic, json.dumps(payload, ensure_ascii=False), qos=1)
        log.info("回复 %s | success=%s data=%s", topic, success, data)

    # ---------- 上报 ----------
    def _publish_report(self, changed):
        self.seq += 1
        payload = {
            "messageId": str(uuid.uuid4()),
            "timestamp": int(time.time() * 1000),
            "properties": {
                "temperature": round(self.temperature, 1),
                "humidity": round(self.humidity, 1),
                "report_interval": self.interval,
            },
        }
        info = self.mqtt.publish(self.topic_report, json.dumps(payload, ensure_ascii=False),
                                 qos=self.qos)
        log.info("属性上报 -> %s | 温度=%.1f 湿度=%.1f | 变化=%s (mid=%s)",
                 self.topic_report, self.temperature, self.humidity, changed, info.mid)

    def _check_alarm(self):
        """温湿度越限检查, 冷却周期内只报一次"""
        self.alarm_cycle += 1
        if self.alarm_cycle % self.alarm_cfg["cooldown"] != 0:
            return
        alarms = []
        if self.temperature > self.alarm_cfg["temp_max"]:
            alarms.append(("temperature_high", self.temperature,
                           "温度 %.1f℃ 超过上限 %.1f℃" % (self.temperature, self.alarm_cfg["temp_max"])))
        elif self.temperature < self.alarm_cfg["temp_min"]:
            alarms.append(("temperature_low", self.temperature,
                           "温度 %.1f℃ 低于下限 %.1f℃" % (self.temperature, self.alarm_cfg["temp_min"])))
        if self.humidity > self.alarm_cfg["hum_max"]:
            alarms.append(("humidity_high", self.humidity,
                           "湿度 %.1f%% 超过上限 %.1f%%" % (self.humidity, self.alarm_cfg["hum_max"])))
        elif self.humidity < self.alarm_cfg["hum_min"]:
            alarms.append(("humidity_low", self.humidity,
                           "湿度 %.1f%% 低于下限 %.1f%%" % (self.humidity, self.alarm_cfg["hum_min"])))
        for atype, value, message in alarms:
            payload = {"type": atype, "value": round(value, 1), "message": message,
                       "timestamp": int(time.time() * 1000)}
            self.mqtt.publish(self.topic_event_alarm, json.dumps(payload, ensure_ascii=False), qos=1)
            log.info("事件上报 -> %s | %s", self.topic_event_alarm, payload)

    # ---------- 模拟主循环 ----------
    def _random_walk(self, value, lo, hi):
        step = random.uniform(-self.max_step, self.max_step)
        return max(lo, min(hi, value + step))

    def run(self):
        log.info("传感器模拟器(JetLinks直连)启动: 产品=%s 设备=%s | 温度 %.1f°C / 湿度 %.1f%% | 周期 %ss",
                 self.product_id, self.device_id, self.temperature, self.humidity, self.interval)
        self._connect()
        time.sleep(1)

        last_t, last_h = self.temperature, self.humidity
        while self.running:
            # 断线重连(每次用新时间戳重新生成认证)
            if self.mqtt is None or not self.mqtt.is_connected():
                log.warning("MQTT 未连接, 重新连接...")
                self._teardown()
                time.sleep(1)
                self._connect()
                time.sleep(2)
                continue

            time.sleep(self.interval)
            if not self.running:
                break

            self.temperature = round(self._random_walk(self.temperature, self.t_min, self.t_max), 1)
            self.humidity = round(self._random_walk(self.humidity, self.h_min, self.h_max), 1)
            self._save_state()
            self._check_alarm()

            changed = []
            if abs(self.temperature - last_t) >= self.deadband:
                changed.append("temperature")
            if abs(self.humidity - last_h) >= self.deadband:
                changed.append("humidity")
            if changed:
                self._publish_report(changed)
                last_t, last_h = self.temperature, self.humidity

        self._teardown()
        log.info("模拟器已退出, 最终状态已保存到 %s", self.json_file)

    def stop(self, *_args):
        self.running = False


def main():
    cfg = load_config()
    sim = SensorSimulatorJetLinks(cfg)
    signal.signal(signal.SIGINT, sim.stop)
    signal.signal(signal.SIGTERM, sim.stop)
    sim.run()


if __name__ == "__main__":
    sys.exit(main())
