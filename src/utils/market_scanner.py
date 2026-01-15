import json
import logging
import os
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional

try:
    from vnstock import listing_companies  # type: ignore
except Exception:  # pragma: no cover - phụ thuộc phiên bản vnstock
    listing_companies = None  # type: ignore[assignment]

from vnstock import Listing

from src.utils.file_utils import ensure_dir
from src.utils.logger import setup_logger


class MarketScanner:
    """Quét danh sách mã chứng khoán đang niêm yết trên thị trường Việt Nam."""

    def __init__(self, cache_dir: str = 'data/cache') -> None:
        """
        Khởi tạo MarketScanner.

        Args:
            cache_dir: Thư mục dùng để lưu cache danh sách mã.
        """

        # Cho phép override nhanh cache dir qua biến môi trường (hữu ích khi chạy CI/cron)
        env_cache_dir = os.getenv('MDP_CACHE_DIR')
        self.cache_dir = Path(env_cache_dir) if env_cache_dir else Path(cache_dir)
        self.cache_file = self.cache_dir / 'all_tickers_cache.json'
        ensure_dir(str(self.cache_dir))
        self.logger = setup_logger(self.__class__.__name__)
        self._last_raw_data: Optional[List[Dict]] = None

    def get_all_tickers(self, force_refresh: bool = False) -> List[str]:
        """
        Trả về danh sách mã chứng khoán (đã lọc & sort).

        Logic:
        - Nếu cache hợp lệ và không force_refresh -> trả về từ cache.
        - Nếu không -> gọi API vnstock, lọc, lưu cache, trả về.
        - Nếu API lỗi nhưng cache cũ tồn tại -> log warning và trả cache cũ.
        - Nếu API lỗi và không có cache -> raise RuntimeError.

        Args:
            force_refresh: Bỏ qua cache và gọi API mới.

        Returns:
            Danh sách mã chứng khoán (unique, sorted A-Z).
        """

        if not force_refresh and self._is_cache_valid():
            cached = self._load_cache()
            if cached:
                self.logger.info('Sử dụng cache tickers: %s', self.cache_file)
                return cached

        try:
            tickers = self._fetch_from_api()
            tickers = self._filter_tickers(tickers, raw_data=self._last_raw_data)
            # Dùng set() để loại bỏ trùng lặp và sort alphabet trước khi trả về
            tickers = sorted(set(tickers))
            if not tickers:
                raise RuntimeError('API trả về danh sách tickers rỗng sau khi lọc.')
            self._save_cache(tickers)
            return tickers
        except Exception as exc:
            cached = self._load_cache()
            if cached:
                self.logger.warning(
                    'Gọi API thất bại (%s). Dùng cache cũ: %s', exc, self.cache_file
                )
                return cached
            raise RuntimeError(f'Không thể lấy danh sách tickers từ API và không có cache: {exc}') from exc

    def get_cache_info(self) -> Optional[Dict]:
        """
        Lấy thông tin cache hiện tại.

        Returns:
            Dict chứa `created_date` và `count` nếu cache tồn tại, ngược lại None.
        """

        if not self.cache_file.exists():
            return None
        try:
            payload = json.loads(self.cache_file.read_text(encoding='utf-8'))
            return {
                'created_date': payload.get('created_date'),
                'count': len(payload.get('tickers', []) or []),
                'path': str(self.cache_file),
                'checked_at': datetime.utcnow().isoformat(),
            }
        except Exception as exc:
            self.logger.warning('Không đọc được cache info: %s', exc)
            return None

    def clear_cache(self) -> bool:
        """
        Xóa file cache hiện tại.

        Returns:
            True nếu xóa thành công hoặc file không tồn tại; False nếu lỗi.
        """

        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
            return True
        except Exception as exc:
            self.logger.error('Xóa cache thất bại: %s', exc)
            return False

    def _is_cache_valid(self) -> bool:
        """
        Kiểm tra cache hợp lệ:
        - File tồn tại
        - `created_date` trong cache == ngày hôm nay

        Returns:
            True nếu hợp lệ, False nếu không.
        """

        if not self.cache_file.exists():
            return False
        try:
            payload = json.loads(self.cache_file.read_text(encoding='utf-8'))
            created = payload.get('created_date')
            return created == date.today().isoformat()
        except Exception:
            return False

    def _fetch_raw_from_api(self) -> List[Dict]:
        """(Deprecated) Giữ tương thích ngược: trả về raw_data dạng List[Dict]."""

        if listing_companies is not None:
            self.logger.info('Gọi vnstock listing_companies để lấy danh sách mã...')
            try:
                raw = listing_companies(exchange='all')
                normalized = self._normalize_raw(raw)
                if normalized:
                    return normalized
            except Exception as exc_all:
                self.logger.warning('listing_companies(exchange="all") thất bại: %s', exc_all)

            combined: List[Dict] = []
            for exch in ['HOSE', 'HNX', 'UPCOM']:
                try:
                    raw = listing_companies(exchange=exch)
                    combined.extend(self._normalize_raw(raw))
                except Exception as exc:
                    self.logger.warning('listing_companies(exchange="%s") thất bại: %s', exch, exc)

            if combined:
                return combined

        # Fallback theo API class Listing (đã được sử dụng trong các module khác của dự án)
        self.logger.info('Fallback: dùng Listing().symbols_by_exchange() để lấy danh sách mã...')
        listing = Listing(source='vci')
        df = listing.symbols_by_exchange()
        try:
            df = df.query('type == "STOCK"')  # chỉ lấy cổ phiếu, tránh các loại khác
        except Exception:
            pass
        return df.to_dict(orient='records')

    def _fetch_from_api(self) -> List[str]:
        """
        Gọi API vnstock lấy danh sách tickers từ HOSE, HNX, UPCOM.

        Returns:
            Danh sách mã chứng khoán (chưa lọc ETF/status).
        """

        raw_data = self._fetch_raw_from_api()
        self._last_raw_data = raw_data
        return self._extract_tickers(raw_data)

    def _normalize_raw(self, raw: object) -> List[Dict]:
        """
        Chuẩn hóa output của vnstock về List[Dict].

        Args:
            raw: Output từ vnstock (có thể là list[dict], dict, hoặc DataFrame).

        Returns:
            List[Dict]
        """

        try:
            import pandas as pd  # local import để giảm phụ thuộc khi không cần

            if isinstance(raw, pd.DataFrame):
                return raw.to_dict(orient='records')
        except Exception:
            pass

        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        if isinstance(raw, dict):
            return [raw]
        return []

    def _extract_tickers(self, raw_data: List[Dict]) -> List[str]:
        """
        Trích xuất ticker từ raw_data.

        Vì field có thể khác nhau theo phiên bản, thử theo thứ tự:
        - 'symbol'
        - 'ticker'
        - 'code'
        - 'stock_code'
        """

        tickers: List[str] = []
        for item in raw_data:
            for key in ['symbol', 'ticker', 'code', 'stock_code']:
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    tickers.append(value.strip().upper())
                    break
        return tickers

    def _filter_tickers(self, tickers: List[str], raw_data: Optional[List[Dict]] = None) -> List[str]:
        """
        Lọc danh sách tickers:
        - Loại ETF theo prefix (VF, FUE, E1VF, SSV)
        - Loại mã không hoạt động nếu có field `status`

        Args:
            tickers: Danh sách ticker đầu vào.
            raw_data: Dữ liệu thô từ API (nếu có) để dùng lọc theo status.

        Returns:
            Danh sách ticker đã lọc.
        """

        etf_prefixes = ('VF', 'FUE', 'E1VF', 'SSV')
        etf_filtered = [t for t in tickers if not t.startswith(etf_prefixes)]

        if not raw_data:
            return etf_filtered

        # Lọc status nếu có: giữ các status được coi là đang hoạt động.
        # Vì nguồn dữ liệu có thể khác nhau, ưu tiên logic "exclude" những status rõ ràng là inactive.
        inactive_keywords = {'delist', 'inactive', 'suspended', 'halt', 'stop'}
        active: List[str] = []
        status_map: Dict[str, str] = {}
        for item in raw_data:
            symbol = None
            for key in ['symbol', 'ticker', 'code', 'stock_code']:
                if isinstance(item.get(key), str):
                    symbol = item.get(key).strip().upper()
                    break
            if not symbol:
                continue
            status_val = item.get('status')
            if isinstance(status_val, str):
                status_map[symbol] = status_val.strip().lower()

        for ticker in etf_filtered:
            status = status_map.get(ticker)
            if status and any(k in status for k in inactive_keywords):
                continue
            active.append(ticker)

        return active

    def _save_cache(self, tickers: List[str]) -> bool:
        """
        Lưu danh sách tickers vào cache JSON.

        Format:
        {
          "created_date": "YYYY-MM-DD",
          "tickers": [...]
        }
        """

        try:
            ensure_dir(str(self.cache_dir))
            payload = {'created_date': date.today().isoformat(), 'tickers': tickers}
            self.cache_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            self.logger.info('Đã lưu cache tickers: %s (%s mã)', self.cache_file, len(tickers))
            return True
        except Exception as exc:
            self.logger.warning('Lưu cache thất bại: %s', exc)
            return False

    def _load_cache(self) -> List[str]:
        """Đọc danh sách tickers từ cache (nếu có)."""

        if not self.cache_file.exists():
            return []
        try:
            payload = json.loads(self.cache_file.read_text(encoding='utf-8'))
            tickers = payload.get('tickers', []) or []
            if not isinstance(tickers, list):
                return []
            return sorted(set(str(t).strip().upper() for t in tickers if str(t).strip()))
        except Exception as exc:
            self.logger.warning('Đọc cache thất bại: %s', exc)
            return []
