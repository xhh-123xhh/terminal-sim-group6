"""日志模块：基于 logging 的彩色控制台 + 滚动文件双输出

统一配置 root logger，让所有子 logger（modbus/mqtt/channels 等）
都能正确输出，无需各自注册 handler。
"""
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional

_ROOT_CONFIGURED = False


def setup_logger(
    name: str = "simulator",
    level: str = "INFO",
    log_file: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """配置 root logger 并返回指定名称的 logger，重复调用幂等"""
    global _ROOT_CONFIGURED

    lvl = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()

    if not _ROOT_CONFIGURED:
        root.setLevel(lvl)
        root.propagate = False

        fmt = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # 控制台
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)

        # 文件（可选）
        if log_file:
            os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
            fh = RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
            )
            fh.setFormatter(fmt)
            root.addHandler(fh)

        _ROOT_CONFIGURED = True
    else:
        # 已配置过，仅调整级别
        root.setLevel(lvl)

    return logging.getLogger(name)


def get_logger(name: str = "simulator") -> logging.Logger:
    """获取子 logger（继承 root 的 handler）"""
    return logging.getLogger(name)