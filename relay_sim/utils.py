"""通用工具：位运算 / 时间戳 / JSON 序列化 / 字典取值"""
import json
import time
from typing import Any, Dict, List, Optional


def now_ts() -> int:
    """秒级时间戳"""
    return int(time.time())


def now_ms() -> int:
    """毫秒级时间戳"""
    return int(time.time() * 1000)


def to_json(payload: Any) -> str:
    """紧凑 JSON 序列化（ensure_ascii=False 保留中文）"""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def safe_get(d: Optional[Dict], *keys, default=None):
    """嵌套 dict 安全取值"""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def int_to_bitfield(values: List[int], nbits: int) -> int:
    """把 0/1 列表编码成一个 int（bit0 = values[0]）
    超出 nbits 的位被截断；输入值非 0/1 视为 0
    """
    mask = 0
    for i, v in enumerate(values[:nbits]):
        if v:
            mask |= (1 << i)
    return mask & ((1 << nbits) - 1)


def bitfield_to_list(value: int, nbits: int) -> List[int]:
    """把 int 按位拆成 0/1 列表，下标 0 = bit0 = 通道1"""
    return [(value >> i) & 1 for i in range(nbits)]


def mask_to_bit(value: int, bit_index: int) -> bool:
    """取某一位是否为 1"""
    return bool((value >> bit_index) & 1)


def set_bit(value: int, bit_index: int, on: bool) -> int:
    """设置某一位"""
    if on:
        return value | (1 << bit_index)
    return value & ~(1 << bit_index)


def build_topic(template: str, **kwargs) -> str:
    """用 kwargs 替换 {key} 占位符"""
    out = template
    for k, v in kwargs.items():
        out = out.replace("{" + k + "}", str(v))
    return out