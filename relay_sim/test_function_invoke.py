"""端到端测试：平台 function/invoke 功能调用 -> 模拟器 -> Modbus 寄存器 -> 回复

模拟平台下发 FunctionInvokeMessage，验证模拟器：
1. 收到 function/invoke 消息
2. 解析 inputs 数组 [{name,value}]
3. 写 Modbus 寄存器6
4. 回复 function/invoke/reply
"""
import json
import threading
import time

import paho.mqtt.client as mqtt
from pymodbus.client import ModbusTcpClient

BROKER = "172.16.4.211"
PORT = 9783
USER = "test"
PASS = "123456"
PID = "relay-8ch-group6"
DID = "relay-sim-group6-001"

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


def read_reg():
    mc = ModbusTcpClient("127.0.0.1", port=5502)
    mc.connect()
    rr = mc.read_holding_registers(6, count=1, device_id=1)
    mc.close()
    if rr.isError():
        return None
    return rr.registers[0]


def main():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(USER, PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 30)
    client.loop_start()
    time.sleep(2)  # 等订阅生效

    print("\n=== 功能调用端到端测试 ===")
    print(f"1. 初始寄存器6 = {hex(read_reg())}")

    # 下发功能调用：设置 ch1=开, ch3=开
    msg_id = "test-invoke-001"
    invoke = {
        "timestamp": int(time.time() * 1000),
        "messageId": msg_id,
        "deviceId": DID,
        "functionId": "set",
        "inputs": [
            {"name": "ch1", "value": True},
            {"name": "ch3", "value": True},
        ],
    }
    client.publish(TOPIC_INVOKE, json.dumps(invoke), qos=1)
    print(f"2. 已发布 function/invoke: {json.dumps(invoke, ensure_ascii=False)}")

    if reply_received.wait(timeout=5):
        print(f"3. 收到回复: {json.dumps(reply_payload, ensure_ascii=False, indent=1)}")
    else:
        print("3. [失败] 5秒内未收到 function/invoke/reply！")

    time.sleep(1)
    reg = read_reg()
    print(f"4. 下发后寄存器6 = {hex(reg) if reg is not None else 'ERR'}  (期望 0x5 = bit0+bit2)")

    # 再下发一次复位 ch1
    reply_received.clear()
    msg_id2 = "test-invoke-002"
    invoke2 = {
        "timestamp": int(time.time() * 1000),
        "messageId": msg_id2,
        "deviceId": DID,
        "functionId": "set",
        "inputs": [
            {"name": "ch1", "value": False},
        ],
    }
    client.publish(TOPIC_INVOKE, json.dumps(invoke2), qos=1)
    if reply_received.wait(timeout=5):
        print(f"5. 复位回复: {json.dumps(reply_payload, ensure_ascii=False)}")
    else:
        print("5. [失败] 复位未收到回复")

    time.sleep(1)
    reg = read_reg()
    print(f"6. 复位后寄存器6 = {hex(reg) if reg is not None else 'ERR'}  (期望 0x4 = bit2)")

    client.loop_stop()
    print("\n=== 测试完成 ===")


if __name__ == "__main__":
    main()