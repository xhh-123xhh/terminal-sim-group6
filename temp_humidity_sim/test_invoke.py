"""端到端测试：平台 function/invoke 功能调用 -> 模拟器 -> 回复

模拟平台下发 FunctionInvokeMessage，验证模拟器：
1. 收到 function/invoke 消息
2. 解析 inputs 数组 [{id,value}]
3. 执行功能（set_report_interval / reset / calibrate_offset）
4. 回复 function/invoke/reply

前置条件：先启动 main.py（模拟器需在运行中）。
"""
import json
import threading
import time

import paho.mqtt.client as mqtt

BROKER = "172.16.4.211"
PORT = 9783
USER = "test"
PASS = "123456"
PID = "th_sensor_6"
DID = "dev_th_6_01"

TOPIC_INVOKE = f"/{PID}/{DID}/function/invoke"
TOPIC_REPLY = f"/{PID}/{DID}/function/invoke/reply"

reply_received = threading.Event()
reply_payload = {}


def on_connect(client, userdata, flags, reason_code, properties=None):
    client.subscribe(TOPIC_REPLY, qos=1)
    print(f"[订阅] {TOPIC_REPLY}")


def on_message(client, userdata, msg):
    global reply_payload
    if msg.topic == TOPIC_REPLY:
        reply_payload = json.loads(msg.payload.decode("utf-8"))
        reply_received.set()


def invoke(client, function_id, inputs):
    """下发一次功能调用并等待回复"""
    global reply_payload
    reply_received.clear()
    msg_id = f"test-{function_id}-{int(time.time()*1000)}"
    payload = {
        "timestamp": int(time.time() * 1000),
        "messageId": msg_id,
        "deviceId": DID,
        "functionId": function_id,
        "inputs": inputs,
    }
    client.publish(TOPIC_INVOKE, json.dumps(payload, ensure_ascii=False), qos=1)
    print(f"[下发] {function_id} inputs={inputs}")
    if reply_received.wait(timeout=5):
        print(f"[回复] {json.dumps(reply_payload, ensure_ascii=False)}")
        return reply_payload
    print(f"[失败] 5 秒内未收到 {function_id} 的回复！")
    return None


def main():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(USER, PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 30)
    client.loop_start()
    time.sleep(2)  # 等订阅生效

    print("\n=== 温湿度功能调用端到端测试 ===")

    # 1. 设置上报周期
    invoke(client, "set_report_interval", [{"id": "interval", "value": 10}])

    # 2. 校准偏移
    invoke(client, "calibrate_offset", [
        {"id": "temp_offset", "value": 1.0},
        {"id": "hum_offset", "value": -2.0},
    ])

    # 3. 重置
    invoke(client, "reset", [])

    client.loop_stop()
    print("\n=== 测试完成 ===")


if __name__ == "__main__":
    main()
