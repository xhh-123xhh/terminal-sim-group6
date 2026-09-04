"""端到端测试：平台 function/invoke 功能调用 -> 模拟器 -> 回复

模拟平台下发 FunctionInvokeMessage，验证模拟器：
1. 收到 function/invoke 消息
2. 解析 inputs 数组 [{id,value}]
3. 执行功能（set_report_interval / calibrate_offset / reset）
4. 回复 function/invoke/reply（字段为 output，非 data）

重点回归「校准偏移」：
  - 校准偏移应持久累加（连调两次 temp_offset=+1 → temp_offset=+2）
  - 校准后显示值应 = 原始值 + 偏移，且不会被下一次随机游走覆盖
  - reset 后偏移清零

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
TOPIC_REPORT = f"/{PID}/{DID}/properties/report"

reply_received = threading.Event()
reply_payload = {}
report_payload = {}


def on_connect(client, userdata, flags, reason_code, properties=None):
    client.subscribe(TOPIC_REPLY, qos=1)
    client.subscribe(TOPIC_REPORT, qos=1)
    print(f"[订阅] {TOPIC_REPLY} / {TOPIC_REPORT}")


def on_message(client, userdata, msg):
    global reply_payload, report_payload
    if msg.topic == TOPIC_REPLY:
        reply_payload = json.loads(msg.payload.decode("utf-8"))
        reply_received.set()
    elif msg.topic == TOPIC_REPORT:
        report_payload = json.loads(msg.payload.decode("utf-8"))


def invoke(client, function_id, inputs, timeout=5):
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
    if reply_received.wait(timeout=timeout):
        print(f"[回复] {json.dumps(reply_payload, ensure_ascii=False)}")
        return reply_payload
    print(f"[失败] {timeout} 秒内未收到 {function_id} 的回复！")
    return None


def check(name, cond):
    print(f"  {'✅' if cond else '❌'} {name}")
    return bool(cond)


def main():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(USER, PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 30)
    client.loop_start()
    time.sleep(2)  # 等订阅生效

    print("\n=== 温湿度功能调用端到端测试 ===")
    all_ok = True

    # 1. 设置上报周期
    r = invoke(client, "set_report_interval", [{"id": "interval", "value": 10}])
    all_ok &= check("set_report_interval 成功且 output.interval=10",
                    r and r.get("success") and r.get("output", {}).get("interval") == 10)

    # 2. 校准偏移（连调两次，验证持久累加）
    # 注意：平台实际下发的 inputs 用 name 字段（非 id），这里用 name 模拟真实场景
    r1 = invoke(client, "calibrate_offset", [
        {"name": "temp_offset", "value": 1.0},
        {"name": "hum_offset", "value": -2.0},
    ])
    all_ok &= check("第一次校准成功", r1 and r1.get("success"))
    all_ok &= check("回复含 output 字段(非 data)", r1 and "output" in r1 and "data" not in r1)
    all_ok &= check("回复含 functionId 字段", r1 and r1.get("functionId") == "calibrate_offset")
    o1 = (r1 or {}).get("output", {})

    r2 = invoke(client, "calibrate_offset", [
        {"name": "temp_offset", "value": 1.0},
        {"name": "hum_offset", "value": 0.0},
    ])
    o2 = (r2 or {}).get("output", {})
    all_ok &= check("温度偏移累加 temp_offset=+2", o2.get("temp_offset") == 2.0)
    all_ok &= check("湿度偏移保持不变 hum_offset=-2", o2.get("hum_offset") == -2.0)

    # 3. 校准后显示值 = 原始值 + 偏移（对比两次校准的显示值增量 ≈ +1）
    if "temperature" in o1 and "temperature" in o2:
        delta = round(o2["temperature"] - o1["temperature"], 1)
        all_ok &= check(f"校准后显示温度增量 ≈ +1 (实际 {delta})", abs(delta - 1.0) < 0.3)

    # 4. 重置（偏移清零）
    r3 = invoke(client, "reset", [])
    o3 = (r3 or {}).get("output", {})
    all_ok &= check("reset 成功", r3 and r3.get("success"))
    # reset 后不再含偏移字段，直接再校准一次验证偏移从 0 重新累计
    r4 = invoke(client, "calibrate_offset", [{"name": "temp_offset", "value": 0.5}])
    o4 = (r4 or {}).get("output", {})
    all_ok &= check("reset 后偏移从 0 重新累计 (temp_offset=0.5)", o4.get("temp_offset") == 0.5)

    client.loop_stop()
    print("\n=== 测试完成:", "全部通过 ✅" if all_ok else "存在失败 ❌", "===")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
