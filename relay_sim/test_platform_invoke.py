"""通过平台 API 真实调用功能，验证「平台 UI -> MQTT function/invoke -> 模拟器 -> 回复」完整链路

监听两个主题：
- function/invoke：平台下发的功能调用（真实下发，不是我们自己发的）
- function/invoke/reply：模拟器回复
"""
import json
import threading
import time

import paho.mqtt.client as mqtt
import requests
from pymodbus.client import ModbusTcpClient

BROKER = "172.16.4.211"
PORT = 9783
USER = "test"
PASS = "123456"
PID = "relay-8ch-group6"
DID = "relay-sim-group6-001"

BASE = "http://172.16.4.211:8848"

TOPIC_INVOKE = f"/{PID}/{DID}/function/invoke"
TOPIC_REPLY = f"/{PID}/{DID}/function/invoke/reply"

got_invoke = threading.Event()
got_reply = threading.Event()
invoke_msg = {}
reply_msg = {}


def on_connect(client, userdata, flags, reason_code, properties=None):
    client.subscribe(TOPIC_INVOKE, qos=1)
    client.subscribe(TOPIC_REPLY, qos=1)
    print(f"[订阅] {TOPIC_INVOKE}")
    print(f"[订阅] {TOPIC_REPLY}")


def on_message(client, userdata, msg):
    global invoke_msg, reply_msg
    try:
        p = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        p = {"raw": msg.payload.decode("utf-8", "ignore")}
    if msg.topic == TOPIC_INVOKE:
        invoke_msg = p
        got_invoke.set()
    elif msg.topic == TOPIC_REPLY:
        reply_msg = p
        got_reply.set()


def read_reg():
    mc = ModbusTcpClient("127.0.0.1", port=5502)
    mc.connect()
    rr = mc.read_holding_registers(6, count=1, device_id=1)
    mc.close()
    return None if rr.isError() else rr.registers[0]


def main():
    # 登录平台
    r = requests.post(BASE + "/authorize/login",
                      json={"username": "admin6", "password": "Admin@group6"}, timeout=10)
    T = r.json()["result"]["token"]
    H = {"Authorization": "Bearer " + T, "Content-Type": "application/json"}

    # MQTT 监听
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(USER, PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 30)
    client.loop_start()
    time.sleep(2)

    print(f"\n=== 平台 API 功能调用测试 ===")
    print(f"1. 初始寄存器6 = {hex(read_reg())}")

    # 调用平台 API：设置 ch2=开, ch4=开（body 直接传参数 map）
    body = {"ch2": True, "ch4": True}
    r = requests.post(BASE + f"/device/instance/{DID}/function/set",
                      headers=H, json=body, timeout=15)
    print(f"2. POST function/set body={json.dumps(body, ensure_ascii=False)}")
    print(f"   API 响应: {r.status_code} {r.text[:300]}")

    # 等平台下发
    if got_invoke.wait(timeout=6):
        print(f"3. 收到平台下发 function/invoke: {json.dumps(invoke_msg, ensure_ascii=False)}")
    else:
        print("3. [失败] 6秒内未收到平台下发的 function/invoke")

    # 等模拟器回复
    if got_reply.wait(timeout=6):
        print(f"4. 收到模拟器回复: {json.dumps(reply_msg, ensure_ascii=False)}")
    else:
        print("4. [失败] 6秒内未收到模拟器 function/invoke/reply")

    time.sleep(1)
    reg = read_reg()
    print(f"5. 下发后寄存器6 = {hex(reg) if reg is not None else 'ERR'}  (期望 0xA = bit1+bit3)")

    # 复位 ch2
    got_invoke.clear(); got_reply.clear()
    body2 = {"ch2": False}
    requests.post(BASE + f"/device/instance/{DID}/function/set", headers=H, json=body2, timeout=15)
    got_invoke.wait(timeout=6)
    got_reply.wait(timeout=6)
    time.sleep(1)
    reg = read_reg()
    print(f"6. 复位后寄存器6 = {hex(reg) if reg is not None else 'ERR'}  (期望 0x8 = bit3)")

    client.loop_stop()
    print("\n=== 测试完成 ===")


if __name__ == "__main__":
    main()