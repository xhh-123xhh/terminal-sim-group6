"""MQTT 客户端：对接 JetLinks 物联网平台

按 JetLinks "MQTT Broker接入"(mqtt-client-gateway) 默认协议实现：
- 属性上报：/{productId}/{deviceId}/properties/report
  负载：{"timestamp": ms, "messageId": uuid, "properties": {...}}
- 命令下发(写属性)：/{productId}/{deviceId}/properties/write
- 命令响应：/{productId}/{deviceId}/properties/write/reply
- 上线/下线：/{productId}/{deviceId}/online|offline

paho-mqtt 2.x 注意：
- 必须用 Callback API v2
- connect/loop 都可用，paho 自动起后台线程
"""
import json
import threading
import time
import uuid
from typing import Callable, Dict, Optional

import paho.mqtt.client as mqtt

from channels import ChannelManager
from logger import get_logger
from utils import build_topic, now_ms, to_json


class JetLinksMQTTClient:
    """JetLinks MQTT 客户端

    主题构建规则按 config.yaml 的模板。
    """

    def __init__(self, cfg: dict, channels: ChannelManager):
        self.cfg = cfg
        self.channels = channels
        self.log = get_logger("mqtt")

        mqtt_cfg = cfg["mqtt"]
        device_cfg = cfg["device"]

        self.broker = mqtt_cfg["broker"]
        self.port = int(mqtt_cfg["port"])
        self.username = mqtt_cfg["username"]
        self.password = mqtt_cfg["password"]
        self.keepalive = int(mqtt_cfg.get("keepalive", 30))
        self.qos = int(mqtt_cfg.get("qos", 1))

        prefix = mqtt_cfg.get("client_id_prefix", "device")
        self.client_id = f"{prefix}-{device_cfg['device_id']}"

        # 主题模板
        t = mqtt_cfg["topics"]
        self.product_id = device_cfg["product_id"]
        self.device_id = device_cfg["device_id"]
        self.topic_property_post = build_topic(t["property_post"],
                                               product_id=self.product_id, device_id=self.device_id)
        self.topic_event_post = build_topic(t["event_post"],
                                            product_id=self.product_id, device_id=self.device_id)
        self.topic_service_set = build_topic(t["service_set"],
                                             product_id=self.product_id, device_id=self.device_id)
        self.topic_service_invoke = build_topic(t["service_invoke"],
                                                product_id=self.product_id, device_id=self.device_id)
        self.topic_invoke_reply = build_topic(t["invoke_reply"],
                                              product_id=self.product_id, device_id=self.device_id)
        self.topic_set_reply = build_topic(t["set_reply"],
                                           product_id=self.product_id, device_id=self.device_id)
        self.topic_online = build_topic(t["online"],
                                        product_id=self.product_id, device_id=self.device_id)
        self.topic_offline = build_topic(t["offline"],
                                         product_id=self.product_id, device_id=self.device_id)

        # paho 客户端（v2 API）
        self.client = mqtt.Client(
            client_id=self.client_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self.client.username_pw_set(self.username, self.password)

        # 回调
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe

        # 状态
        self._connected = threading.Event()
        self._lock = threading.Lock()
        self._stopped = False

        # 命令处理器（外部注入）
        self._command_handler: Optional[Callable[[dict, str], None]] = None

    # ---------- 公共接口 ----------
    def set_command_handler(self, handler: Callable[[dict, str], None]):
        """注入命令处理器：handler(payload: dict, msg_id: str) -> None"""
        self._command_handler = handler

    def connect(self):
        """连接 broker（后台线程）"""
        self.log.info("connecting to mqtt://%s:%d as %s", self.broker, self.port, self.client_id)
        try:
            self.client.connect(self.broker, self.port, self.keepalive)
        except Exception as e:
            self.log.error("mqtt connect error: %s", e)
            raise
        self.client.loop_start()

    def stop(self):
        self._stopped = True
        # 优雅下线消息
        try:
            if self._connected.is_set():
                self.publish_offline()
        except Exception:
            pass
        try:
            self.client.disconnect()
        except Exception:
            pass
        try:
            self.client.loop_stop()
        except Exception:
            pass
        self.log.info("mqtt client stopped")

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def wait_connected(self, timeout: float = 5.0) -> bool:
        return self._connected.wait(timeout=timeout)

    # ---------- 发布 ----------
    def publish_property(self, extra: Optional[dict] = None):
        """发布属性上报：8 路继电器状态

        JetLinks mqtt-client-gateway 默认协议格式：
        {"timestamp": ms, "messageId": uuid, "properties": {"ch1_state": true, ...}}
        """
        payload = self.channels.get_all_dict()
        payload["online"] = True
        if extra:
            payload.update(extra)
        msg = {
            "timestamp": now_ms(),
            "messageId": str(uuid.uuid4()),
            "properties": payload,
        }
        body = to_json(msg)
        info = self.client.publish(self.topic_property_post, body, qos=self.qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            self.log.warning("publish property failed rc=%s", info.rc)
        else:
            self.log.info("published property: %s", body)

    def publish_set_reply(self, msg_id: str, code: int = 200, msg: str = "ok"):
        """回复平台命令响应"""
        reply = {
            "messageId": msg_id,
            "success": code == 200,
            "code": code,
            "msg": msg,
            "timestamp": now_ms(),
        }
        body = to_json(reply)
        info = self.client.publish(self.topic_set_reply, body, qos=self.qos)
        self.log.info("set_reply: id=%s code=%d body=%s", msg_id, code, body)

    def publish_invoke_reply(self, msg_id: str, function_id: str, output=None,
                             success: bool = True, msg: str = "ok"):
        """回复平台功能调用结果（function/invoke/reply）"""
        reply = {
            "timestamp": now_ms(),
            "messageId": msg_id,
            "functionId": function_id,
            "output": output,
            "success": success,
        }
        if not success and msg:
            reply["message"] = msg
        body = to_json(reply)
        info = self.client.publish(self.topic_invoke_reply, body, qos=self.qos)
        self.log.info("invoke_reply: id=%s functionId=%s success=%s body=%s",
                      msg_id, function_id, success, body)

    def publish_online(self):
        """上线消息（will 之外，主动发一次）"""
        body = to_json({"online": True, "ts": now_ms()})
        info = self.client.publish(self.topic_online, body, qos=self.qos)
        self.log.info("published online")

    def publish_offline(self):
        """下线消息"""
        body = to_json({"online": False, "ts": now_ms()})
        info = self.client.publish(self.topic_offline, body, qos=self.qos)
        self.log.info("published offline")

    # ---------- 回调 ----------
    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0 or (hasattr(reason_code, "is_failure") and not reason_code.is_failure):
            self.log.info("mqtt connected")
            self._connected.set()
            # 订阅命令主题
            client.subscribe(self.topic_service_set, qos=self.qos)
            # 订阅通配主题（接收任何 service/*/invoke）
            client.subscribe(self.topic_service_invoke, qos=self.qos)
            self.log.info("subscribed: %s | %s", self.topic_service_set, self.topic_service_invoke)
            # 上线消息
            self.publish_online()
        else:
            self.log.error("mqtt connect failed rc=%s", reason_code)
            self._connected.clear()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self.log.warning("mqtt disconnected rc=%s", reason_code)
        self._connected.clear()

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties=None):
        self.log.debug("subscribed mid=%s", mid)

    def _on_message(self, client, userdata, msg):
        """收到 MQTT 消息"""
        try:
            body = msg.payload.decode("utf-8")
            self.log.info("recv topic=%s payload=%s", msg.topic, body)
        except Exception as e:
            self.log.error("decode error: %s", e)
            return

        try:
            payload = json.loads(body)
        except Exception as e:
            self.log.error("json parse error: %s", e)
            return

        if msg.topic == self.topic_service_set:
            # 属性设置命令
            self._handle_set_command(payload)
        elif msg.topic == self.topic_service_invoke:
            # 功能调用（function invoke）
            self._handle_invoke_command(payload)
        else:
            self.log.debug("ignore topic: %s", msg.topic)

    def _handle_set_command(self, payload: dict):
        """处理属性写入命令

        JetLinks mqtt-client-gateway 默认协议格式：
        {
            "timestamp": 1585811800000,
            "messageId": "xxx",
            "properties": {"ch1_state": true, ...}
        }
        """
        msg_id = payload.get("messageId") or payload.get("id", "unknown")
        params = payload.get("properties", {})
        if not isinstance(params, dict):
            self.log.warning("invalid properties: %s", params)
            self.publish_set_reply(msg_id, code=400, msg="invalid properties")
            return

        # 找出 ch*_state 字段
        ch_payload = {k: v for k, v in params.items() if k.startswith("ch") and k.endswith("_state")}

        if not ch_payload:
            self.log.warning("no ch state params in command: %s", params)
            self.publish_set_reply(msg_id, code=400, msg="no ch state params")
            return

        # 更新 channels
        self.log.info("apply set command id=%s params=%s", msg_id, ch_payload)
        if self._command_handler:
            try:
                self._command_handler(ch_payload, msg_id)
            except Exception as e:
                self.log.exception("command handler error: %s", e)
                self.publish_set_reply(msg_id, code=500, msg=str(e))
        else:
            # 默认处理：直接更新 channels
            self.channels.set_channels_from_dict(ch_payload, source="mqtt")
            self.publish_set_reply(msg_id, code=200, msg="ok")

    # ---------- 功能调用（function invoke） ----------
    def _handle_invoke_command(self, payload: dict):
        """处理平台下发的功能调用

        JetLinks 官方协议 function/invoke 下行格式：
        {
            "timestamp": 1601196762389,
            "messageId": "xxx",
            "deviceId": "...",
            "functionId": "set",
            "inputs": [{"name": "ch1", "value": true}, ...]   # 数组；也兼容 {name: value} 对象
        }
        """
        msg_id = payload.get("messageId") or payload.get("id", "unknown")
        function_id = payload.get("functionId") or payload.get("function", "unknown")
        inputs = payload.get("inputs", [])

        # 兼容 inputs 为 {name: value} 对象的形式
        if isinstance(inputs, dict):
            inputs = [{"name": k, "value": v} for k, v in inputs.items()]

        if not isinstance(inputs, list):
            self.log.warning("invalid inputs in function invoke: %s", inputs)
            self.publish_invoke_reply(msg_id, function_id, success=False, msg="invalid inputs")
            return

        # 把参数名统一映射成 ch{n}_state
        ch_payload = {}
        for item in inputs:
            if not isinstance(item, dict):
                continue
            name = item.get("name") or item.get("id")
            value = item.get("value")
            if name is None:
                continue
            key = self._normalize_channel_key(str(name))
            if key:
                ch_payload[key] = value

        if not ch_payload:
            self.log.warning("no channel params in function invoke: %s", payload)
            self.publish_invoke_reply(msg_id, function_id, success=False, msg="no channel params")
            return

        self.log.info("apply function invoke id=%s functionId=%s params=%s",
                      msg_id, function_id, ch_payload)
        self.channels.set_channels_from_dict(ch_payload, source="mqtt-function")
        # 返回执行结果：当前 8 路状态
        output = self.channels.get_all_dict()
        self.publish_invoke_reply(msg_id, function_id, output=output, success=True, msg="ok")

    @staticmethod
    def _normalize_channel_key(name: str):
        """把 ch1 / ch1_state 统一成 ch1_state；非法返回 None"""
        name = name.strip()
        if name.endswith("_state"):
            return name if name.startswith("ch") else None
        if name.startswith("ch"):
            try:
                idx = int(name[2:])
                return f"ch{idx}_state"
            except ValueError:
                return None
        return None