import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


class JsonFormatter(logging.Formatter):
    """Định dạng bản ghi dạng JSON để ghi ra khi cần."""

    def __init__(self, datefmt: str = '%Y-%m-%dT%H:%M:%S%z') -> None:
        super().__init__(datefmt=datefmt)

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'module': record.name,
            'message': record.getMessage(),
        }
        if record.exc_info:
            payload['exception'] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class LoggerConfig:
    """Cấu hình logging dùng cho toàn bộ ứng dụng."""

    def __init__(
        self,
        log_dir: str = 'logs',
        log_filename: str = 'app.log',
        level: str = 'INFO',
        json_output: Optional[bool] = None,
        json_path: Optional[str] = None,
    ) -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.log_dir / log_filename
        self.level = getattr(logging, level.upper(), logging.INFO)
        self.json_output = (
            json_output
            if json_output is not None
            else os.getenv('LOG_JSON_OUTPUT', 'false').lower() in ('1', 'true', 'yes')
        )
        self.json_path = (
            Path(json_path) if json_path else self.log_dir / 'app.json'
        )
        self._formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        self._json_formatter = JsonFormatter()

    def _configure_console(self) -> logging.Handler:
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setLevel(self.level)
        console_handler.setFormatter(self._formatter)
        return console_handler

    def _configure_file(self) -> logging.Handler:
        file_handler = RotatingFileHandler(
            filename=str(self.log_path),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8',
        )
        file_handler.setLevel(self.level)
        file_handler.setFormatter(self._formatter)
        return file_handler

    def _configure_json(self) -> logging.Handler:
        json_handler = RotatingFileHandler(
            filename=str(self.json_path),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding='utf-8',
        )
        json_handler.setLevel(self.level)
        json_handler.setFormatter(self._json_formatter)
        return json_handler

    def configure(self) -> None:
        """Áp dụng cấu hình logging cho root logger."""

        root_logger = logging.getLogger()
        root_logger.setLevel(self.level)
        for handler in list(root_logger.handlers):
            try:
                handler.close()
            finally:
                root_logger.removeHandler(handler)
        root_logger.addHandler(self._configure_console())
        root_logger.addHandler(self._configure_file())
        if self.json_output:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            self.json_path.parent.mkdir(parents=True, exist_ok=True)
            root_logger.addHandler(self._configure_json())


_GLOBAL_CONFIG: Optional[LoggerConfig] = None


def configure_logging(
    log_dir: str = 'logs',
    log_filename: str = 'app.log',
    level: str = 'INFO',
    json_output: Optional[bool] = None,
    json_path: Optional[str] = None,
) -> LoggerConfig:
    """Thiết lập cấu hình logging toàn cục, có thể gọi lại để thay đổi nơi ghi log."""

    try:
        sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]
    except Exception:
        pass

    global _GLOBAL_CONFIG
    _GLOBAL_CONFIG = LoggerConfig(
        log_dir=log_dir,
        log_filename=log_filename,
        level=level,
        json_output=json_output,
        json_path=json_path,
    )
    _GLOBAL_CONFIG.configure()
    return _GLOBAL_CONFIG


def get_logger(module_name: str) -> logging.Logger:
    """Trả về logger theo module name, sử dụng cấu hình global hiện tại."""

    global _GLOBAL_CONFIG
    if _GLOBAL_CONFIG is None:
        configure_logging()
    return logging.getLogger(module_name)


def setup_logger(
    module_name: str,
    log_dir: str = 'logs',
    log_filename: str = 'app.log',
    level: str = 'INFO',
) -> logging.Logger:
    """Thiết lập logging (nếu chưa) và trả về logger theo module."""

    global _GLOBAL_CONFIG
    if _GLOBAL_CONFIG is None:
        configure_logging(log_dir=log_dir, log_filename=log_filename, level=level)
    return get_logger(module_name)
