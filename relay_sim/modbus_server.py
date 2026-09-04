"""Modbus TCP 从站

设计：
- 用 pymodbus 3.15 的新 API（SimData/SimDevice + action 回调）
- 寄存器地址 0-5 是只读占位区（项目历史约定 0x0000-0x0009）
- 寄存器地址 6 的 16-bit 值表示 8 路继电器状态（bit0~bit7 → 通道1~8）
- action 回调在每次读写时被调用，负责：
    * 读请求：把 channels 当前状态写入 registers 返回
    * 写请求：解析写入值为 8 路继电器状态，调用 channels.set_state_bits
      （channels.on_change 回调会自动触发 MQTT 上报）
- 这样 channel 是唯一事实源，Modbus 寄存器是它的实时投影
"""
import threading
from typing import Optional

from pymodbus.constants import ExcCodes
from pymodbus.datastore.sequential import DataType
from pymodbus.server import StartTcpServer
from pymodbus.simulator import SimData, SimDevice

from channels import ChannelManager
from logger import get_logger


# Modbus 功能码
FC_READ_HOLDING = 3
FC_WRITE_SINGLE = 6
FC_WRITE_MULTI = 16


class ModbusServer:
    """Modbus TCP 从站

    数据布局（保持寄存器 0x0000-0x0009 的项目约定）：
        0-5: 只读占位（filler）
        6:   8 路继电器 bit 字段（bit0=ch1, ..., bit7=ch8）
    """

    # Modbus 功能码常量
    FC_COIL_R = 1
    FC_DI_R = 2
    FC_HR_R = 3
    FC_IR_R = 4
    FC_COIL_W = 5
    FC_HR_W = 6
    FC_HR_W_MULTI = 16

    def __init__(self, cfg: dict, channels: ChannelManager):
        self.cfg = cfg
        self.channels = channels
        self.log = get_logger("modbus")

        host = cfg.get("host", "0.0.0.0")
        port = int(cfg.get("port", 5502))
        self.host = host
        self.port = port

        # 继电器状态（启动初始值）
        initial_bits = self.channels.get_state_bits() & 0xFF

        # 寄存器布局：地址 0-5 只读占位，地址 6 可读写
        self.filler = SimData(
            address=0, count=6, values=0, datatype=DataType.UINT16, readonly=True
        )
        self.relays = SimData(
            address=6, count=1, values=initial_bits,
            datatype=DataType.UINT16, readonly=False,
        )
        # 设备配置 action 回调
        self.device = SimDevice(
            id=1,
            simdata=[self.filler, self.relays],
            action=self._on_register_access,
        )

        self._thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

    # ---------- 生命周期 ----------
    def start(self):
        if self._thread and self._thread.is_alive():
            self.log.warning("modbus server already running")
            return
        self._stop_flag.clear()
        self._thread = threading.Thread(
            target=self._serve_forever, name="modbus-server", daemon=True
        )
        self._thread.start()
        self.log.info("modbus server started on %s:%d (reg 6 = 8ch relays)",
                      self.host, self.port)

    def stop(self, timeout: float = 3.0):
        self._stop_flag.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        self.log.info("modbus server stopped")

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _serve_forever(self):
        try:
            # StartTcpServer 是阻塞调用
            StartTcpServer(context=self.device, address=(self.host, self.port))
        except Exception as e:
            self.log.exception("modbus server crashed: %s", e)

    # ---------- action 回调 ----------
    async def _on_register_access(
        self,
        func_code: int,
        start_address: int,
        address: int,
        count: int,
        registers: list[int],
        set_values: Optional[list[int]],
    ):
        """Modbus 寄存器访问回调

        - 读请求（set_values is None）：把 channels 当前状态写入 registers
        - 写请求（set_values not None）：解析为 8 路继电器状态，更新 channels
        - 越界访问地址 0-5：返回 ILLEGAL_ADDRESS
        """
        # 计算相对偏移
        offset = address - start_address
        # 我们只关心 holding register 区域
        if func_code not in (self.FC_HR_R, self.FC_HR_W, self.FC_HR_W_MULTI):
            return None  # 其他类型不拦截

        if set_values is None:
            # 读请求：把 channels 状态投影到 registers
            bits = self.channels.get_state_bits() & 0xFF
            # 我们的 holding register 从 address 0 开始
            # 寄存器 0-5 是 filler，寄存器 6 是继电器
            for i in range(count):
                addr = address + i
                if addr == 6:
                    registers[offset + i] = bits
                # 0-5 是 filler，registers 已有正确初始值
            return None
        else:
            # 写请求：解析并更新 channels
            for i, v in enumerate(set_values):
                addr = address + i
                if addr == 6:
                    bits = int(v) & 0xFF
                    if self.channels.set_state_bits(bits, source="modbus"):
                        self.log.info("modbus wrote reg 6 = 0x%02X (ch state changed)", bits)
                elif 0 <= addr <= 5:
                    # filler 区只读，拒绝
                    return ExcCodes.ILLEGAL_ADDRESS
                else:
                    return ExcCodes.ILLEGAL_ADDRESS
            return None