"""8路继电器通道状态管理

核心设计：
- 8路继电器状态用 Modbus 寄存器地址 6 的低 8 bit 表示
  - bit0 = 通道1, bit1 = 通道2, ..., bit7 = 通道8
  - 0 = 关(OFF), 1 = 开(ON)
- 维护一个统一的"事实源"（state_bits），所有读写都通过它
- 提供锁保证线程安全（Modbus 异步任务 / MQTT 回调可能在不同线程）
- 提供回调机制，状态变化时通知外部（用于触发上报等）
"""
import threading
from typing import Callable, Dict, List, Optional, Tuple

from utils import bitfield_to_list, int_to_bitfield, mask_to_bit, set_bit


class ChannelManager:
    """8路继电器状态管理器

    state_bits: int —— 0~0xFF，bit0~bit7 对应 8 个通道
    """

    def __init__(
        self,
        nbits: int = 8,
        initial: Optional[List[int]] = None,
        bit_map: Optional[Dict[str, int]] = None,
    ):
        self.nbits = nbits
        if initial is None:
            initial = [0] * nbits
        # 初始化位字段
        if isinstance(initial, int):
            self._state = initial & ((1 << nbits) - 1)
            initial_list = bitfield_to_list(self._state, nbits)
        else:
            self._state = int_to_bitfield(list(initial), nbits)
            initial_list = list(initial[:nbits])

        # 通道位映射（可选，给外部用）
        if bit_map is None:
            bit_map = {str(i + 1): (1 << i) for i in range(nbits)}
        self._bit_map = {int(k): v for k, v in bit_map.items()}

        self._lock = threading.RLock()
        self._callbacks: List[Callable[[int, int, int], None]] = []
        # 日志延迟注入
        self._logger = None

        # 记录初始状态到日志（如果 logger 已注入）
        self._log_state("init", initial_list)

    # ---------- logger 注入 ----------
    def bind_logger(self, logger):
        self._logger = logger

    # ---------- 状态查询 ----------
    def get_state_bits(self) -> int:
        """返回当前 8-bit 状态字（0~0xFF）"""
        with self._lock:
            return self._state

    def get_state_list(self) -> List[int]:
        """返回 0/1 列表，下标 0 = 通道1"""
        with self._lock:
            return bitfield_to_list(self._state, self.nbits)

    def get_channel(self, channel: int) -> int:
        """获取指定通道（1-based）的状态：0/1"""
        if channel < 1 or channel > self.nbits:
            raise ValueError(f"channel must be in 1..{self.nbits}, got {channel}")
        with self._lock:
            return 1 if mask_to_bit(self._state, channel - 1) else 0

    def get_all_dict(self) -> Dict[str, bool]:
        """返回 JetLinks 上报格式：{"ch1_state": true, ...}"""
        with self._lock:
            return {f"ch{i + 1}_state": bool(mask_to_bit(self._state, i)) for i in range(self.nbits)}

    # ---------- 状态写入 ----------
    def set_state_bits(self, new_bits: int, source: str = "unknown") -> bool:
        """原子写入新的位字段，状态变化时触发回调

        返回 True 表示状态确实发生了变化
        """
        new_bits &= (1 << self.nbits) - 1
        with self._lock:
            old = self._state
            if old == new_bits:
                return False
            self._state = new_bits
            changed = old ^ new_bits
        # 锁外触发回调，避免回调内部再次获取锁死锁
        for cb in list(self._callbacks):
            try:
                cb(old, new_bits, changed)
            except Exception as e:  # 回调异常不影响主流程
                if self._logger:
                    self._logger.exception("channel callback error: %s", e)
        self._log_state(source, bitfield_to_list(new_bits, self.nbits), changed_bits=changed)
        return True

    def set_channel(self, channel: int, on: bool, source: str = "unknown") -> bool:
        """设置单通道状态（1-based）"""
        if channel < 1 or channel > self.nbits:
            raise ValueError(f"channel must be in 1..{self.nbits}, got {channel}")
        with self._lock:
            new_bits = set_bit(self._state, channel - 1, on)
        return self.set_state_bits(new_bits, source=source)

    def set_channels_from_dict(self, payload: Dict[str, bool], source: str = "unknown") -> Tuple[int, int]:
        """从 dict 批量更新，返回 (changed_count, new_bits)"""
        with self._lock:
            new_bits = self._state
            for k, v in payload.items():
                if not k.startswith("ch") or not k.endswith("_state"):
                    continue
                try:
                    idx = int(k[2:-6])  # ch1_state -> 1
                except ValueError:
                    continue
                if idx < 1 or idx > self.nbits:
                    continue
                new_bits = set_bit(new_bits, idx - 1, bool(v))
        changed = self.set_state_bits(new_bits, source=source)
        return (1 if changed else 0), self.get_state_bits()

    def set_all(self, on: bool, source: str = "all") -> bool:
        """全开 / 全关"""
        target = (1 << self.nbits) - 1 if on else 0
        return self.set_state_bits(target, source=source)

    # ---------- 回调 ----------
    def on_change(self, callback: Callable[[int, int, int], None]):
        """注册变化回调，签名 (old_bits, new_bits, changed_mask)"""
        self._callbacks.append(callback)

    # ---------- 内部 ----------
    def _log_state(self, source: str, state_list: List[int], changed_bits: int = 0):
        if not self._logger:
            return
        ch_repr = " ".join(f"ch{i + 1}={'ON' if v else 'OFF'}" for i, v in enumerate(state_list))
        if changed_bits:
            chg = " ".join(
                f"ch{i + 1}={'→ON' if mask_to_bit(changed_bits, i) else ''}"
                for i in range(self.nbits) if mask_to_bit(changed_bits, i)
            )
            self._logger.info("[%s] %s  (changed: %s) bits=0x%02X", source, ch_repr, chg.strip(), self._state)
        else:
            self._logger.debug("[%s] %s bits=0x%02X", source, ch_repr, self._state)