import csv
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List

import pandas as pd

LOGGER = logging.getLogger(__name__)


def ensure_dir(path: str) -> bool:
    """Tạo thư mục (và các thư mục cha) nếu chưa tồn tại."""

    directory = Path(path)
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        LOGGER.error('Không thể tạo thư mục %s: %s', path, exc)
        return False
    return True


def atomic_write_csv(filepath: str, data: List[Dict[str, Any]], **kwargs: Any) -> bool:
    """Ghi CSV một cách an toàn, đảm bảo không ghi đè đang viết."""

    path = Path(filepath)
    if not ensure_dir(str(path.parent)):
        return False
    temp_file = None
    try:
        temp_file = NamedTemporaryFile(
            mode='w',
            newline='',
            delete=False,
            dir=str(path.parent),
            suffix='.tmp',
            encoding=kwargs.pop('encoding', 'utf-8'),
        )
        if data:
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(temp_file, fieldnames=fieldnames, **kwargs)
            writer.writeheader()
            writer.writerows(data)
        else:
            temp_file.write('')  # tạo file rỗng nếu không có dữ liệu
        temp_file.flush()
        os.fsync(temp_file.fileno())
        temp_file.close()
        os.replace(temp_file.name, path)
        return True
    except Exception as exc:
        LOGGER.error('Ghi CSV không thành công %s: %s', filepath, exc)
        return False
    finally:
        if temp_file and not temp_file.closed:
            temp_file.close()


def safe_read_csv(filepath: str) -> pd.DataFrame:
    """Đọc CSV có xử lý lỗi trả về DataFrame rỗng nếu thất bại."""

    try:
        return pd.read_csv(filepath)
    except Exception as exc:
        LOGGER.warning('Không đọc được file %s: %s', filepath, exc)
        return pd.DataFrame()


def get_file_size(filepath: str) -> int:
    """Trả về kích thước file tính bằng byte."""

    path = Path(filepath)
    try:
        return path.stat().st_size
    except Exception as exc:
        LOGGER.warning('Không lấy được kích thước file %s: %s', filepath, exc)
        return 0


def clean_old_files(directory: str, days: int) -> int:
    """Xóa file cũ hơn số ngày cho trước và trả về số file đã xóa."""

    cutoff = datetime.utcnow() - timedelta(days=days)
    removed = 0
    dir_path = Path(directory)
    if not dir_path.exists():
        return removed
    for item in dir_path.iterdir():
        try:
            if item.is_file() and datetime.utcfromtimestamp(item.stat().st_mtime) < cutoff:
                item.unlink()
                removed += 1
        except Exception as exc:
            LOGGER.warning('Không thể xóa %s: %s', item, exc)
    return removed
