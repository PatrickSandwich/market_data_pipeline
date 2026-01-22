from __future__ import annotations

import re
from typing import Dict, List, Tuple

from vnstock import Quote


def validate_and_filter_symbols(raw_symbols: List[str]) -> Tuple[List[str], List[Dict]]:
    """
    Validate và lọc danh sách symbols để tránh làm hỏng toàn bộ pipeline.

    Strategy:
    - Chuẩn hóa: strip + uppercase.
    - Check format cơ bản: 3-5 ký tự chữ/số (VN tickers có thể chứa số, ví dụ: A32).
    - Thử khởi tạo `Quote(symbol=...)` để bắt các lỗi khởi tạo (nếu có).

    Args:
        raw_symbols: Danh sách symbols thô từ API/Cache.

    Returns:
        Tuple[List[str], List[Dict]]:
            - valid_symbols: symbols hợp lệ (unique, giữ nguyên thứ tự tương đối).
            - removed_symbols_with_reasons: danh sách symbols bị loại cùng lý do.
    """

    valid_symbols: List[str] = []
    removed_symbols: List[Dict] = []
    seen = set()

    for symbol in raw_symbols:
        normalized = (symbol or '').strip().upper()
        if not normalized:
            removed_symbols.append(
                {
                    'symbol': symbol,
                    'reason': 'Empty symbol',
                    'error_type': 'ValueError',
                }
            )
            continue
        if normalized in seen:
            continue

        if not re.match(r'^[A-Z0-9]{3,5}$', normalized):
            removed_symbols.append(
                {
                    'symbol': normalized,
                    'reason': 'Invalid symbol format (expected 3-5 alphanumeric)',
                    'error_type': 'FormatError',
                }
            )
            continue

        try:
            # Lưu ý: Nhiều phiên bản vnstock không gọi network ở bước init,
            # nhưng đây vẫn là check rẻ để bắt lỗi input/symbol mapping.
            Quote(symbol=normalized, source='vci')
            valid_symbols.append(normalized)
            seen.add(normalized)
        except Exception as exc:
            removed_symbols.append(
                {
                    'symbol': normalized,
                    'reason': str(exc),
                    'error_type': type(exc).__name__,
                }
            )

    return valid_symbols, removed_symbols
