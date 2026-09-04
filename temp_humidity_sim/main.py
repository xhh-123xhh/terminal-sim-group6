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

校准设计 (重要):
    模拟器内部维护「原始值」(raw) 与「校准偏移」(offset) 两个量:
        显示值 = 原始值 + 偏移
    随机游走只在「原始值」上跑，校准/重置只改「偏移」。
    这样校准一次会持续生效，不会被下一次随机游走覆盖。
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
        # 显示值的合法范围（对应物模型 valueType 的 min/max）
        self.display_temp_range = scfg.get("display_temp_range", [0.0, 100.0])
        self.display_hum_range = scfg.get("display_hum_range", [0.0, 100.0])
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
        # 原始值（随机游走在此之上跑）
        self.temperature = state["temperature"]
        self.humidity = state["humidity"]
        # 校准偏移（持久化，校准/重置只改这里）
        self.temp_offset = state.get("temp_offset", 0.0)
        self.hum_offset = state.get("hum_offset", 0.0)

        self.mqtt = None

    # ---------- 显示值 = 原始值 + 偏移 ----------
    def _display_temperature(self):
        lo, hi = self.display_temp_range
        return round(max(lo, min(hi, self.temperature + self.temp_offset)), 1)

    def _display_humidity(self):
        lo, hi = self.display_hum_range
        return round(max(lo, min(hi, self.humidity + self.hum_offset)), 1)

    # ---------- JSON 持久化 ----------
    def _load_state(self):
        if os.path.exists(self.json_file):
            try:
                with open(self.json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return {
                    "temperature": float(data["temperature"]),
                    "humidity": float(data["humidity"]),
                    # 兼容旧版无偏移字段的文件
                    "temp_offset": float(data.get("temp_offset", 0.0)),
                    "hum_offset": float(data.get("hum_offset", 0.0)),
                }
            except Exception as e:
                log.warning("JSON 状态文件损坏, 使用随机初值: %s", e)
        return {
            "temperature": round(random.uniform(self.t_min, self.t_max), 1),
            "humidity": round(random.uniform(self.h_min, self.h_max), 1),
            "temp_offset": 0.0,
            "hum_offset": 0.0,
        }

    def _save_state(self):
        payload = {
            "device_id": self.device_id,
            "product_id": self.product_id,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "temperature": round(self.temperature, 1),
            "humidity": round(self.humidity, 1),
            "temp_offset": round(self.temp_offset, 1),
            "hum_offset": round(self.hum_offset, 1),
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

    @staticmethod
    def _parse_inputs(raw_inputs):
        """兼容 JetLinks 平台多种 inputs 结构:
          平铺(name 字段, 平台 API 实际下发):  [{"name":"temp_offset","value":2.0}]
          平铺(id 字段, MQTT 直发):            [{"id":"interval","value":30}]
          嵌套:                                [{"name":"params","value":[{"id":"interval","value":30}]}]
        返回 {参数名: value} 字典。key 优先取 id，其次 name。
        """
        if not raw_inputs:
            return {}
        if isinstance(raw_inputs[0], dict) and raw_inputs[0].get("name") == "params":
            raw_inputs = raw_inputs[0].get("value") or []
        result = {}
        for i in raw_inputs:
            if not isinstance(i, dict):
                continue
            key = i.get("id") or i.get("name")
            if key is not None:
                result[key] = i.get("value")
        return result

    @staticmethod
    def _as_float(value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _handle_function_invoke(self, msg):
        """平台功能调用"""
        mid = msg.get("messageId")
        fid = msg.get("functionId")
        inputs = self._parse_inputs(msg.get("inputs"))
        log.info("收到功能调用: %s inputs=%s", fid, inputs)

        success, output = False, {}
        if fid == "set_report_interval":
            v = max(1, self._as_int(inputs.get("interval"), self.interval))
            self.interval = v
            success, output = True, {"interval": v}
        elif fid == "calibrate_offset":
            # 只改偏移量，不改原始值 → 校准持久生效
            new_t_offset = self._as_float(inputs.get("temp_offset"), 0.0)
            new_h_offset = self._as_float(inputs.get("hum_offset"), 0.0)
            self.temp_offset = round(self.temp_offset + new_t_offset, 1)
            self.hum_offset = round(self.hum_offset + new_h_offset, 1)
            self._save_state()
            success, output = True, {
                "temperature": self._display_temperature(),
                "humidity": self._display_humidity(),
                "temp_offset": self.temp_offset,
                "hum_offset": self.hum_offset,
            }
            log.info("校准偏移已更新: temp_offset=%+.1f hum_offset=%+.1f (显示值 %.1f°C / %.1f%%)",
                     self.temp_offset, self.hum_offset,
                     self._display_temperature(), self._display_humidity())
        elif fid == "reset":
            self.temperature = round(random.uniform(self.t_min, self.t_max), 1)
            self.humidity = round(random.uniform(self.h_min, self.h_max), 1)
            self.temp_offset = 0.0
            self.hum_offset = 0.0
            self._save_state()
            success, output = True, {
                "temperature": self._display_temperature(),
                "humidity": self._display_humidity(),
            }
            log.info("已重置: 原始值=%.1f°C/%.1f%% 偏移清零", self.temperature, self.humidity)
        else:
            log.warning("未知功能: %s", fid)

        self._reply_function(mid, fid, success, output,
                             error=None if success else "unknown function: %s" % fid)
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
            # 兼容 id / name 两种字段
            parsed = {}
            for i in props:
                if isinstance(i, dict):
                    key = i.get("id") or i.get("name")
                    if key is not None:
                        parsed[key] = i.get("value")
            props = parsed
        applied = {}
        if "report_interval" in props:
            self.interval = max(1, self._as_int(props["report_interval"], self.interval))
            applied["report_interval"] = self.interval
        # 平台直接写绝对值 → 视为「重新标定」，清除原有偏移
        if "temperature" in props:
            self.temperature = round(self._as_float(props["temperature"]), 1)
            self.temp_offset = 0.0
            applied["temperature"] = self._display_temperature()
        if "humidity" in props:
            self.humidity = round(self._as_float(props["humidity"]), 1)
            self.hum_offset = 0.0
            applied["humidity"] = self._display_humidity()
        if applied:
            self._save_state()
            log.info("属性修改已应用: %s", applied)
        self._reply_property_write(mid, True)
        if applied:
            self._publish_report(changed="after-write")

    def _reply_function(self, mid, fid, success, output, error=None):
        if not mid:
            return
        payload = {
            "messageId": mid,
            "functionId": fid,
            "output": output,
            "success": success,
            "timestamp": int(time.time() * 1000),
        }
        if error:
            payload["message"] = error
        self.mqtt.publish(self.topic_fn_reply, json.dumps(payload, ensure_ascii=False), qos=1)
        log.info("功能回复 -> %s | functionId=%s success=%s output=%s",
                 self.topic_fn_reply, fid, success, output)

    def _reply_property_write(self, mid, success):
        if not mid:
            return
        payload = {
            "messageId": mid,
            "success": success,
            "timestamp": int(time.time() * 1000),
        }
        self.mqtt.publish(self.topic_prop_write_reply, json.dumps(payload, ensure_ascii=False), qos=1)
        log.info("属性写回复 -> %s | success=%s", self.topic_prop_write_reply, success)

    # ---------- 上报 ----------
    def _publish_report(self, changed):
        self.seq += 1
        payload = {
            "messageId": str(uuid.uuid4()),
            "timestamp": int(time.time() * 1000),
            "properties": {
                "temperature": self._display_temperature(),
                "humidity": self._display_humidity(),
                "report_interval": self.interval,
            },
        }
        info = self.mqtt.publish(self.topic_report, json.dumps(payload, ensure_ascii=False),
                                 qos=self.qos)
        log.info("属性上报 -> %s | 温度=%.1f 湿度=%.1f | 变化=%s (mid=%s)",
                 self.topic_report, self._display_temperature(), self._display_humidity(),
                 changed, info.mid)

    def _check_alarm(self):
        """温湿度越限检查(基于显示值), 冷却周期内只报一次"""
        self.alarm_cycle += 1
        if self.alarm_cycle % self.alarm_cfg["cooldown"] != 0:
            return
        temp = self._display_temperature()
        hum = self._display_humidity()
        alarms = []
        if temp > self.alarm_cfg["temp_max"]:
            alarms.append(("temperature_high", temp,
                           "温度 %.1f℃ 超过上限 %.1f℃" % (temp, self.alarm_cfg["temp_max"])))
        elif temp < self.alarm_cfg["temp_min"]:
            alarms.append(("temperature_low", temp,
                           "温度 %.1f℃ 低于下限 %.1f℃" % (temp, self.alarm_cfg["temp_min"])))
        if hum > self.alarm_cfg["hum_max"]:
            alarms.append(("humidity_high", hum,
                           "湿度 %.1f%% 超过上限 %.1f%%" % (hum, self.alarm_cfg["hum_max"])))
        elif hum < self.alarm_cfg["hum_min"]:
            alarms.append(("humidity_low", hum,
                           "湿度 %.1f%% 低于下限 %.1f%%" % (hum, self.alarm_cfg["hum_min"])))
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
        log.info("传感器模拟器(JetLinks直连)启动: 产品=%s 设备=%s | 显示值 %.1f°C / %.1f%% (偏移 %+.1f / %+.1f) | 周期 %ss",
                 self.product_id, self.device_id,
                 self._display_temperature(), self._display_humidity(),
                 self.temp_offset, self.hum_offset, self.interval)
        self._connect()
        time.sleep(1)

        last_t, last_h = self._display_temperature(), self._display_humidity()
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

            # 随机游走只作用于「原始值」
            self.temperature = round(self._random_walk(self.temperature, self.t_min, self.t_max), 1)
            self.humidity = round(self._random_walk(self.humidity, self.h_min, self.h_max), 1)
            self._save_state()
            self._check_alarm()

            # 死区判断基于「显示值」
            cur_t, cur_h = self._display_temperature(), self._display_humidity()
            changed = []
            if abs(cur_t - last_t) >= self.deadband:
                changed.append("temperature")
            if abs(cur_h - last_h) >= self.deadband:
                changed.append("humidity")
            if changed:
                self._publish_report(changed)
                last_t, last_h = cur_t, cur_h

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
